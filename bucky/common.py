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


import os
import io
import sys
import time
import string
import socket
import signal
import random
import logging
import multiprocessing
import bucky.cfg as cfg


def load_config(config_file, module_name=None):
    new_config = {}
    with open(config_file or cfg.__file__, 'r') as f:
        config_template = string.Template(f.read())
        config_str = config_template.substitute(os.environ)
        exec(config_str, new_config)

    if module_name:
        # Module specific config requested, if exists, should override the globals
        if module_name in new_config:
            module_config = new_config.pop(module_name)
            new_config.update(module_config)

    unused_config_keys = set(vars(cfg).keys()) - set(new_config.keys())
    for k, v in new_config.items():
        setattr(cfg, k, v)
    for k in unused_config_keys:
        if not k.startswith('_'):
            delattr(cfg, k)


def setup_logging(module_name=None):
    if module_name:
        # Reinit those in subprocesses to avoid races on the underlying streams
        sys.stdout = io.TextIOWrapper(io.FileIO(1, mode='wb', closefd=False))
        sys.stderr = io.TextIOWrapper(io.FileIO(2, mode='wb', closefd=False))
    root = logging.getLogger(module_name)
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(getattr(cfg, 'log_level', 'INFO'))
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)-15s][%(levelname)s] %(name)s(%(process)d) - %(message)s")
    handler.setFormatter(formatter)
    root.addHandler(handler)
    return logging.getLogger(module_name)


class MetricsProcess(multiprocessing.Process):
    def __init__(self, module_name, config_file):
        super().__init__(name=module_name, daemon=True)
        self.config_file = config_file
        self.buffer = []
        self.next_flush = 0

    def schedule_tick(self):
        now = time.monotonic()
        while now + 0.3 >= self.next_tick:
            self.next_tick += self.tick_interval
        signal.setitimer(signal.ITIMER_REAL, self.next_tick - now)

    def tick_handler(self, signal_number, stack_frame):
        self.log.debug("Tick signal received")
        self.tick()
        self.schedule_tick()

    def setup_tick(self):
        self.tick_interval = getattr(cfg, 'flush_interval', None) or None
        if self.tick_interval:
            self.next_tick = time.monotonic() + self.tick_interval
            signal.signal(signal.SIGALRM, self.tick_handler)
            signal.setitimer(signal.ITIMER_REAL, self.tick_interval)

    def tick(self):
        now = time.monotonic()
        if now < self.next_flush:
            # self.log.debug("Flush held back, now is %f, should be at least %f", now, self.next_flush)
            return
        if self.flush():
            self.flush_interval = self.tick_interval or 1
            self.log.debug("Flush succeeded, next in %d secs", int(self.flush_interval))
        else:
            self.flush_interval = self.flush_interval + self.flush_interval
            self.flush_interval = min(self.flush_interval, 600)
            self.log.info("Flush error, next in %d secs", int(self.flush_interval))
        # Occasionally, at least on macOS, kernel wakes up the process a few millis earlier
        # then scheduled (most likely scheduling we do here is introducing some small slips),
        # which triggers the condition at the top of the function and we miss a legit tick.
        # The 0.03s is a harmless margin that seems to solve the problem.
        self.next_flush = now + self.flush_interval - 0.03

    def run(self, loop=True):
        # We have to reset signals set up by master process to reasonable defaults
        # (because we inherited them via fork, which is most likely invalid in our context)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        signal.signal(signal.SIGALRM, signal.SIG_IGN)

        load_config(self.config_file, self.name)
        self.log = setup_logging(self.name)
        self.buffer_limit = getattr(cfg, 'buffer_limit', 10000)
        self.setup_tick()
        self.log.info("Module set up")

        if loop:
            self.loop()

    def loop(self):
        while True:
            time.sleep(60)


class MetricsDstProcess(MetricsProcess):
    def __init__(self, module_name, config_file, src_pipe):
        super().__init__(module_name, config_file)
        self.src_pipe = src_pipe

    def loop(self):
        err = 0
        while True:
            try:
                batch = self.src_pipe.recv()
                self.process_batch(batch)
                err = 0
            except EOFError:
                # This happens when no source is connected up, keep trying for 10s, then give up.
                err += 1
                if err > 10:
                    self.log.error("Input not ready, quitting")
                    return
                time.sleep(1)

    def process_batch(self, batch):
        config_metadata = getattr(cfg, 'metadata', None)

        self.log.debug("Received %d samples", len(batch))

        for sample in batch:
            if len(sample) == 4:
                name, value, timestamp, metadata = sample
            else:
                name, value, timestamp = sample
                metadata = None
            if metadata:
                metadata.update((k, v) for k, v in config_metadata.items() if k not in metadata)
            else:
                metadata = config_metadata

            if type(value) is dict:
                self.process_values(name, value, timestamp, metadata)
            else:
                self.process_value(name, value, timestamp, metadata)

    def process_values(self, name, values, timestamp, metadata=None):
        raise NotImplementedError()

    def process_value(self, name, value, timestamp, metadata=None):
        self.process_values(name, {'value': value}, timestamp, metadata)


class MetricsSrcProcess(MetricsProcess):
    def __init__(self, module_name, config_file, dst_pipes):
        super().__init__(module_name, config_file)
        self.dst_pipes = dst_pipes

    def flush(self):
        if self.buffer:
            for i in self.dst_pipes:
                i.send(self.buffer)
            self.buffer = []
        return True


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
        now = time.monotonic()
        if self.resolved_hosts is None or (now - self.resolved_hosts_timestamp) > 180:
            resolved_hosts = set()
            for host in cfg.remote_hosts:
                for ip, port in self.parse_address(host, self.default_port):
                    self.log.debug("Resolved %s as %s:%d", host, ip, port)
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
            self.log.info("Bound UDP socket %s", str(sock.getsockname()))
        return sock


class TCPConnector(HostResolver):
    def get_tcp_socket(self, bind=False, connect=False):
        ip = getattr(cfg, 'local_host', '0.0.0.0')
        port = getattr(cfg, 'local_port', 0)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if bind:
            sock.bind((ip, port))
            self.log.info("Bound TCP socket %s", str(sock.getsockname()))
        if connect:
            remote_ip, remote_port = random.choice(self.resolve_hosts())
            sock.connect((remote_ip, remote_port))
        return sock


class MetricsPushProcess(MetricsDstProcess):
    def __init__(self, *args, default_port=None):
        super().__init__(*args)
        self.socket = None
        self.default_port = default_port
        self.resolved_hosts = None
        self.resolved_hosts_timestamp = 0

    def process_batch(self, batch):
        super().process_batch(batch)
        self.tick()
        self.trim_buffer()

    def trim_buffer(self):
        if len(self.buffer) > self.buffer_limit:
            self.buffer = self.buffer[-int(self.buffer_limit / 2):]

    def flush(self):
        if len(self.buffer):
            try:
                self.log.debug("Flushing %d entries from buffer", len(self.buffer))
                self.push_buffer()
                self.buffer = []
            except (PermissionError, ConnectionError):
                self.log.exception("Connection error")
                self.socket = None  # Python will trigger close() when GCing it.
                return False
        return True
