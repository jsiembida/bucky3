

import io
import sys
import time
import socket
import signal
import random
import logging
import multiprocessing
import multiprocessing.connection


monotonic_time = time.monotonic
system_time = time.time
sleep = time.sleep


class Logger:
    def setup_logging(self, cfg, module_name=None):
        # Reinit those to avoid races on the underlying streams
        sys.stdout = io.TextIOWrapper(io.FileIO(1, mode='wb', closefd=False))
        sys.stderr = io.TextIOWrapper(io.FileIO(2, mode='wb', closefd=False))
        root = logging.getLogger(module_name)
        for h in list(root.handlers):
            root.removeHandler(h)
        root.setLevel(cfg.get('log_level', 'INFO'))
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            cfg.get('log_format', "[%(asctime)-15s][%(levelname)s] %(name)s(%(process)d) - %(message)s")
        )
        handler.setFormatter(formatter)
        root.addHandler(handler)
        return logging.getLogger(module_name)


class MetricsProcess(multiprocessing.Process, Logger):
    def __init__(self, module_name, module_config):
        super().__init__(name=module_name, daemon=True)
        self.cfg = module_config

    def schedule_tick(self):
        now = monotonic_time()
        while now + 0.3 >= self.next_tick:
            self.next_tick += self.tick_interval
        signal.setitimer(signal.ITIMER_REAL, self.next_tick - now)

    def tick_handler(self, signal_number, stack_frame):
        self.log.debug("Tick received")
        self.tick()
        self.schedule_tick()

    def setup_tick(self):
        if self.tick_interval:
            self.next_tick = monotonic_time() + self.tick_interval
            signal.signal(signal.SIGALRM, self.tick_handler)
            signal.setitimer(signal.ITIMER_REAL, self.tick_interval)

    def tick(self):
        now = monotonic_time()
        if now < self.next_flush:
            return
        if self.flush(now, round(system_time(), 3)):
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

    def init_config(self):
        self.log = self.setup_logging(self.cfg, self.name)
        self.randomize_startup = self.cfg.get('randomize_startup', True)
        self.buffer = []
        self.buffer_limit = max(self.cfg.get('buffer_limit', 10000), 100)
        self.chunk_size = max(self.cfg.get('chunk_size', 300), 1)
        self.tick_interval = self.cfg.get('flush_interval') or None
        self.next_tick = None
        self.flush_interval = self.tick_interval or 1
        self.next_flush = 0
        self.metadata = self.cfg.get('metadata')

    def run(self, loop=True):
        def termination_handler(signal_number, stack_frame):
            self.log.info("Received signal %d, exiting", signal_number)
            sys.exit(0)

        # We have to reset signals set up by master process to reasonable defaults
        # (because we inherited them via fork, which is most likely invalid in our context)
        signal.signal(signal.SIGINT, termination_handler)
        signal.signal(signal.SIGTERM, termination_handler)
        signal.signal(signal.SIGHUP, termination_handler)
        signal.signal(signal.SIGALRM, signal.SIG_IGN)

        self.init_config()
        if self.randomize_startup and self.tick_interval > 3:
            # If randomization is configured (it's default) do it asap, before singal handler gets set up
            sleep(random.randint(0, min(self.tick_interval - 1, 15)))
        self.setup_tick()
        self.log.info("Set up")

        if loop:
            self.loop()

    def loop(self):
        while True:
            try:
                sleep(60)
            except InterruptedError:
                pass


class MetricsDstProcess(MetricsProcess):
    def __init__(self, module_name, module_config, src_pipes):
        super().__init__(module_name, module_config)
        self.src_pipes = src_pipes

    def loop(self):
        err = 0
        while True:
            tmp = False
            for pipe in multiprocessing.connection.wait(self.src_pipes):
                try:
                    batch = pipe.recv()
                    self.process_batch(batch)
                except InterruptedError:
                    pass
                except EOFError:
                    # This happens when no source is connected up, keep trying for 10s, then give up.
                    self.log.debug("EOF while reading source pipe")
                    tmp = True
            err = err + 1 if tmp else 0
            if err > 10:
                self.log.error("Input(s) not ready, quitting")
                return
            elif err:
                sleep(1)

    def process_batch(self, batch):
        for sample in batch:
            if len(sample) == 4:
                bucket, value, timestamp, metadata = sample
            else:
                bucket, value, timestamp = sample
                metadata = None
            if metadata:
                if self.metadata:
                    metadata.update((k, v) for k, v in self.metadata.items() if k not in metadata)
            else:
                metadata = self.metadata

            if type(value) is dict:
                self.process_values(bucket, value, timestamp, metadata)
            else:
                self.process_value(bucket, value, timestamp, metadata)

    def process_values(self, bucket, values, timestamp, metadata=None):
        raise NotImplementedError()

    def process_value(self, bucket, value, timestamp, metadata=None):
        raise NotImplementedError()


class MetricsSrcProcess(MetricsProcess):
    def __init__(self, module_name, module_config, dst_pipes):
        super().__init__(module_name, module_config)
        self.dst_pipes = dst_pipes

    def flush(self, monotonic_timestamp, system_timestamp):
        if self.buffer:
            self.log.debug("Flushing %d entries from buffer", len(self.buffer))
            for chunk_start in range(0, len(self.buffer), self.chunk_size):
                chunk = self.buffer[chunk_start:chunk_start + self.chunk_size]
                for dst in self.dst_pipes:
                    dst.send(chunk)
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
        now = monotonic_time()
        # DNS resolution interval could be parametrized, but it seems a bit involved to get it plugged
        # efficiently into the mixin. The hardcoded 180s on the other hand seems to be reasonable.
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
            if hasattr(socket, 'SO_REUSEPORT'):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
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

    def flush(self, monotonic_timestamp, system_timestamp):
        if self.buffer:
            try:
                self.push_buffer()
                self.buffer.clear()
            except (PermissionError, ConnectionError):
                self.log.exception("Connection error")
                self.socket = None  # CPython will trigger close() when GCing it.
                return False
        return True
