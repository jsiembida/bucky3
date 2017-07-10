

import sys
import time
import socket
import signal
import random
import multiprocessing
import bucky.common as common


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
        self.log.debug("Tick received")
        self.tick()
        self.schedule_tick()

    def setup_tick(self):
        self.tick_interval = self.cfg.get('flush_interval', None) or None
        self.flush_interval = self.tick_interval or 1
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
        def termination_handler(signal_number, stack_frame):
            self.log.info("Received signal %d", signal_number)
            sys.exit(0)

        # We have to reset signals set up by master process to reasonable defaults
        # (because we inherited them via fork, which is most likely invalid in our context)
        signal.signal(signal.SIGINT, termination_handler)
        signal.signal(signal.SIGTERM, termination_handler)
        signal.signal(signal.SIGHUP, termination_handler)
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        signal.signal(signal.SIGALRM, signal.SIG_IGN)

        self.cfg = common.load_config(self.config_file, self.name)
        self.log = common.setup_logging(self.cfg, self.name)
        self.buffer_limit = self.cfg.get('buffer_limit', 10000)
        self.setup_tick()
        self.log.info("Set up")

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
        config_metadata = self.cfg.get('metadata', None)

        for sample in batch:
            if len(sample) == 4:
                bucket, value, timestamp, metadata = sample
            else:
                bucket, value, timestamp = sample
                metadata = None
            if metadata:
                metadata.update((k, v) for k, v in config_metadata.items() if k not in metadata)
            else:
                metadata = config_metadata

            if type(value) is dict:
                self.process_values(bucket, value, timestamp, metadata)
            else:
                self.process_value(bucket, value, timestamp, metadata)

    def process_values(self, bucket, values, timestamp, metadata=None):
        raise NotImplementedError()

    def process_value(self, bucket, value, timestamp, metadata=None):
        self.process_values(bucket, {'value': value}, timestamp, metadata)


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
            for host in self.cfg['remote_hosts']:
                for ip, port in self.parse_address(host, self.default_port):
                    self.log.debug("Resolved %s as %s:%d", host, ip, port)
                    resolved_hosts.add((ip, port))
            resolved_hosts = tuple(resolved_hosts)
            self.resolved_hosts = resolved_hosts
            self.resolved_hosts_timestamp = now
        return self.resolved_hosts


class UDPConnector(HostResolver):
    def get_udp_socket(self, bind=False):
        ip = self.cfg.get('local_host', '0.0.0.0')
        port = self.cfg.get('local_port', 0)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if bind:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((ip, port))
            self.log.info("Bound UDP socket %s:%d", ip, port)
        return sock


class TCPConnector(HostResolver):
    def get_tcp_socket(self, bind=False, connect=False):
        ip = self.cfg.get('local_host', '0.0.0.0')
        port = self.cfg.get('local_port', 0)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if bind:
            sock.bind((ip, port))
            self.log.info("Bound TCP socket %s:%d", ip, port)
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
        buffer_len = len(self.buffer)
        if buffer_len > self.buffer_limit:
            self.buffer = self.buffer[-int(self.buffer_limit / 2):]
            self.log.debug("Buffer trimmed from %d to %d entries", buffer_len, len(self.buffer))

    def flush(self):
        if len(self.buffer):
            try:
                self.push_buffer()
                self.buffer = []
            except (PermissionError, ConnectionError):
                self.log.exception("Connection error")
                self.socket = None  # Python will trigger close() when GCing it.
                return False
        return True
