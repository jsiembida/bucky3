

import io
import sys
import time
import socket
import signal
import random
import logging
import resource
import multiprocessing
import multiprocessing.connection


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
        now = time.monotonic()
        while now + 0.3 >= self.next_tick:
            self.next_tick += self.tick_interval
        signal.setitimer(signal.ITIMER_REAL, self.next_tick - now, self.tick_interval + self.tick_interval)

    def tick_handler(self, signal_number, stack_frame):
        self.log.debug("Tick received")
        self.tick()
        self.schedule_tick()

    def setup_tick(self):
        if self.tick_interval:
            self.next_tick = time.monotonic() + self.tick_interval
            signal.signal(signal.SIGALRM, self.tick_handler)
            signal.setitimer(signal.ITIMER_REAL, self.tick_interval, self.tick_interval + self.tick_interval)

    def tick(self):
        now = time.monotonic()
        if now < self.next_flush:
            return
        if self.flush(round(time.time(), 3)):
            self.flush_interval = self.tick_interval
        else:
            self.flush_interval = min(self.flush_interval + self.flush_interval, 600)
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
        self.tick_interval = max(self.cfg['flush_interval'], 0.1)
        self.flush_interval = self.tick_interval
        self.next_tick = None
        self.next_flush = 0
        self.metadata = self.cfg.get('metadata')
        self.add_timestamps = self.cfg.get('add_timestamps', False)
        self.self_report = self.cfg.get('self_report', False)
        self.self_report_timestamp = 0
        self.init_time = time.monotonic()

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
            time.sleep(random.randint(0, min(self.tick_interval - 1, 15)))
        self.setup_tick()
        self.log.info("Set up")

        if loop:
            self.loop()

    def loop(self):
        while True:
            try:
                time.sleep(60)
            except InterruptedError:
                pass

    def take_self_report(self):
        if not self.self_report:
            return None
        now = time.monotonic()
        if now - self.self_report_timestamp >= 59:
            self.self_report_timestamp = now
            usage = resource.getrusage(resource.RUSAGE_SELF)
            self.process_self_report((
                "bucky3",
                dict(
                    cpu=round(usage.ru_utime + usage.ru_stime, 3),
                    memory=usage.ru_maxrss,
                    uptime=round(time.monotonic() - self.init_time, 3),
                ),
                round(time.time(), 3),
                dict(name=self.name),
            ))

    def process_self_report(self, batch):
        pass


class MetricsDstProcess(MetricsProcess):
    def __init__(self, module_name, module_config, src_pipes):
        super().__init__(module_name, module_config)
        self.src_pipes = src_pipes

    def process_self_report(self, batch):
        self.process_batch([batch])

    def loop(self):
        err = 0
        while True:
            tmp = False
            for pipe in multiprocessing.connection.wait(self.src_pipes, min(self.tick_interval, 60)):
                try:
                    batch = pipe.recv()
                    self.process_batch(batch)
                except InterruptedError:
                    pass
                except EOFError:
                    # This happens when no source is connected up, keep trying for 10s, then give up.
                    self.log.debug("EOF while reading source pipe")
                    tmp = True
            if tmp:
                err = err + 1
            else:
                err = 0
                self.take_self_report()
            if err > 10:
                self.log.error("Input(s) not ready, quitting")
                return
            elif err:
                time.sleep(1)

    # Prometheus2 introduces new semantics of timing out time series and we want it.
    # This only works with metrics with no timestamps provided. So we do just that.
    # We avoid using timestamps as much as possible. InfluxDB and Prometheus take such
    # metrics just fine. For short flush windows we are off by a few secs, so that's ok.
    # For edge cases like very late stats from Cloudwatch of Cloudflare we need
    # to explicitly provide backdated timestamps. For InfluxDB and Prometheus it would
    # make no difference anyway, for Prometheus2 it will revert the timing out logic
    # to the old Prometheus behaviour - and there is no working around it for now.
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

    def tick(self):
        super().tick()
        self.take_self_report()

    def process_self_report(self, batch):
        self.buffer.append(batch)
        self.flush(round(time.time(), 3))

    def flush(self, system_timestamp):
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

    def resolve_host(self, host, default_port):
        for ip, port in self.parse_address(host, default_port):
            self.log.debug("Resolved %s as %s:%d", host, ip, port)
            yield ip, port

    def resolve_local_host(self, default_port=0):
        local_host = self.cfg.get('local_host', '0.0.0.0')
        return random.choice(tuple(self.resolve_host(local_host, default_port)))

    def resolve_remote_hosts(self):
        now = time.monotonic()
        # DNS resolution interval could be parametrized, but it seems a bit involved to get it plugged
        # efficiently into the mixin. The hardcoded 180s on the other hand seems to be reasonable.
        if self.resolved_hosts is None or (now - self.resolved_hosts_timestamp) > 180:
            resolved_hosts = set()
            for host in self.cfg['remote_hosts']:
                resolved_hosts.update(self.resolve_host(host, self.default_port))
            self.resolved_hosts = resolved_hosts
            self.resolved_hosts_timestamp = now
        return self.resolved_hosts


class UDPConnector(HostResolver):
    def get_udp_socket(self, bind=False):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if bind:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, 'SO_REUSEPORT'):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            ip, port = self.resolve_local_host()
            sock.bind((ip, port))
            self.log.info("Bound UDP socket %s:%d", ip, port)
        return sock


class TCPConnector(HostResolver):
    def get_tcp_socket(self, bind=False, connect=False):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if bind:
            ip, port = self.resolve_local_host()
            sock.bind((ip, port))
            self.log.info("Bound TCP socket %s:%d", ip, port)
        if connect:
            remote_ip, remote_port = random.choice(self.resolve_remote_hosts())
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

    def flush(self, system_timestamp):
        if self.buffer:
            try:
                self.push_buffer()
                self.buffer.clear()
            except (PermissionError, ConnectionError):
                self.log.exception("Connection error")
                self.socket = None  # CPython will trigger close() when GCing it.
                return False
        return True


class ProcfsReader:
    INTERFACE_FIELDS = ('rx_bytes', 'rx_packets', 'rx_errors', 'rx_dropped',
                        None, None, None, None,
                        'tx_bytes', 'tx_packets', 'tx_errors', 'tx_dropped')

    def read_interfaces(self, path='/proc/net/dev'):
        with open(path) as f:
            for l in f:
                tokens = l.strip().split()
                if not tokens or len(tokens) != 17:
                    continue
                if not tokens[0].endswith(':'):
                    continue
                interface_name = tokens.pop(0)[:-1]
                interface_stats = {k: int(v) for k, v in zip(self.INTERFACE_FIELDS, tokens) if k}
                yield interface_name, interface_stats

    MEMORY_FIELDS = {
        'MemTotal:': 'total_bytes',
        'MemFree:': 'free_bytes',
        'MemAvailable:': 'available_bytes',
        'Shmem:': 'shared_bytes',
        'Cached:': 'cached_bytes',
        'Slab:': 'slab_bytes',
        'Mapped:': 'mapped_bytes',
        'SwapTotal:': 'swap_total_bytes',
        'SwapFree:': 'swap_free_bytes',
        'SwapCached:': 'swap_cached_bytes',
    }

    def read_memory(self, path='/proc/meminfo'):
        with open(path) as f:
            for l in f:
                tokens = l.strip().split()
                if not tokens or len(tokens) != 3 or tokens[2].lower() != 'kb':
                    continue
                name = tokens[0]
                if name in self.MEMORY_FIELDS:
                    yield self.MEMORY_FIELDS[name], int(tokens[1]) * 1024
