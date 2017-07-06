# -*- coding: utf-8 -
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.


import time
import socket
import signal
import random
import logging
import multiprocessing
import bucky.cfg as cfg


# Would be nicer as a base class inheriting from multiprocessing.Process,
# but we reuse this code in main thread, too. So it is not.
def prepare_module(module_name, config_file, tick_callback, termination_callback=False):
    assert module_name
    next_tick = None

    def load_config(module_section=True):
        if config_file:
            new_config = {}
            with open(config_file, 'r') as f:
                exec(compile(f.read(), config_file, 'exec'), new_config)
        else:
            new_config = vars(cfg)

        if module_section:
            if module_name not in new_config:
                new_config = {}
            else:
                new_config = new_config[module_name]

        unused_config_keys = set(vars(cfg).keys()) - set(new_config.keys())
        for k, v in new_config.items():
            setattr(cfg, k, v)
        for k in unused_config_keys:
            delattr(cfg, k)

        setup_logging()  # Only after this step we have logger configured :-|
        setup_reconfig()
        setup_tick()

    def setup_logging():
        root = logging.getLogger()
        for h in list(root.handlers):
            root.removeHandler(h)
        root.setLevel(getattr(cfg, 'log_level', 'INFO'))
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(asctime)-15s][%(levelname)s] %(name)s(%(process)d) - %(message)s")
        handler.setFormatter(formatter)
        root.addHandler(handler)
        cfg.log = logging.getLogger(module_name)

    def schedule_tick():
        nonlocal next_tick
        if not next_tick:
            return
        interval = getattr(cfg, 'interval', None)
        if not interval:
            return
        next_tick += interval
        now = time.time()
        delay = max(next_tick - now, 0.3)
        signal.setitimer(signal.ITIMER_REAL, delay)

    def setup_tick():
        nonlocal next_tick
        signal.signal(signal.SIGALRM, tick_handler)
        interval = getattr(cfg, 'interval', None)
        if interval:
            next_tick = time.time() + interval
            signal.setitimer(signal.ITIMER_REAL, interval)
        else:
            next_tick = None
            signal.setitimer(signal.ITIMER_REAL, 0)

    def tick_handler(signal_number, stack_frame):
        tick_callback()
        schedule_tick()

    def setup_reconfig():
        signal.signal(signal.SIGHUP, reconfig_handler)

    def reconfig_handler(signal_number, stack_frame):
        load_config()

    def setup_termination():
        signal.signal(signal.SIGINT, termination_handler)
        signal.signal(signal.SIGTERM, termination_handler)

    def termination_handler(signal_number, stack_frame):
        termination_callback()

    if termination_callback:
        load_config(module_section=False)
        setup_termination()
        cfg.log.info("Master module prepared")
    else:
        load_config()
        cfg.log.info("Module %s prepared", module_name)


class MetricsProcess(multiprocessing.Process):
    def __init__(self, module_name, config_file):
        super().__init__(name=module_name, daemon=True)
        self.config_file = config_file
        self.next_interval = 0
        self.next_tick = 0

    def run(self):
        prepare_module(self.name, self.config_file, self.tick)
        self.work()

    def tick(self):
        now = time.time()
        if now >= self.next_tick:
            interval = getattr(cfg, 'interval', 0)
            if self.recoverable_tick():
                self.next_interval = interval
                self.next_tick = now + self.next_interval - 1
            else:
                interval = max(self.next_interval, interval, 1)
                self.next_interval = min(interval + interval, 180)
                self.next_tick = now + self.next_interval
                cfg.log.info("Tick error, next tick in %f secs", self.next_interval)
        else:
            pass

    def recoverable_tick(self):
        return False


class MetricsDstProcess(MetricsProcess):
    def __init__(self, module_name, config_file, src_pipe):
        super().__init__(module_name, config_file)
        self.src_pipe = src_pipe

    def work(self):
        while True:
            samples = self.src_pipe.recv()
            if samples:
                for sample in samples:
                    if type(sample[1]) is dict:
                        self.process_metrics(*sample)
                    else:
                        self.process_metric(*sample)

    def process_metrics(self, name, values, timestamp, metadata=None):
        raise NotImplementedError()

    def process_metric(self, name, value, timestamp, metadata=None):
        self.process_metrics(name, {'value': value}, timestamp, metadata)


class MetricsSrcProcess(MetricsProcess):
    def __init__(self, module_name, config_file, dst_pipes):
        super().__init__(module_name, config_file)
        self.dst_pipes = dst_pipes

    def work(self):
        while True:
            time.sleep(10)

    def send_metrics(self, metrics):
        for i in self.dst_pipes:
            i.send(metrics)


class HostResolver:
    def parse_address(self, address, default_port):
        bits = address.split(":")
        if len(bits) == 1:
            host, port = address, default_port
        elif len(bits) == 2:
            host, port = bits[0], int(bits[1])
        else:
            raise ValueError("Address %s is invalid" % (address,))
        hostname, aliaslist, ipaddrlist = socket.gethostbyname_ex(host)
        return {(ip, port) for ip in ipaddrlist}

    def resolve_hosts(self):
        now = time.time()
        if self.resolved_hosts is None or (now - self.resolved_hosts_timestamp) > 180:
            resolved_hosts = set()
            for host in cfg.remote_hosts:
                for ip, port in self.parse_address(host, self.default_port):
                    cfg.log.debug("Resolved %s as %s:%d", host, ip, port)
                    resolved_hosts.add((ip, port))
            resolved_hosts = tuple(resolved_hosts)
            self.resolved_hosts = resolved_hosts
            self.resolved_hosts_timestamp = now
        return self.resolved_hosts


class UDPConnector(HostResolver):
    def get_udp_socket(self, bind=False):
        ip = getattr(cfg, 'local_host', '0.0.0.0')
        port = getattr(cfg, 'local_port', 0)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if bind:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((ip, port))
            cfg.log.info("Bound UDP socket %s", str(sock.getsockname()))
        return sock


class TCPConnector(HostResolver):
    def get_tcp_socket(self, bind=False, connect=False):
        ip = getattr(cfg, 'local_host', '0.0.0.0')
        port = getattr(cfg, 'local_port', 0)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if bind:
            sock.bind((ip, port))
            cfg.log.info("Bound TCP socket %s", str(sock.getsockname()))
        if connect:
            remote_ip, remote_port = random.choice(self.resolve_hosts())
            sock.connect((remote_ip, remote_port))
        return sock
