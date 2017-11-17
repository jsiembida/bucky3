

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


class Connector:
    def cleanup_socket(self):
        if self.socket:
            self.socket.close()
            self.log.debug('Closed socket')
        self.socket = None


class UDPConnector(Connector, HostResolver):
    def get_udp_socket(self, bind=False):
        # UDP sockets don't need the elaborate recycling TCP sockets do
        if self.socket is None:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.log.info('Created UDP socket')
            if bind:
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if hasattr(socket, 'SO_REUSEPORT'):
                    self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                ip, port = self.resolve_local_host()
                self.socket.bind((ip, port))
                self.log.info("Bound UDP socket %s:%d", ip, port)
        return self.socket


class TCPConnector(Connector, HostResolver):
    # To provide load balancing, when pushing via TCP, we reopen the connection
    # at intervals (using a random host from the pool of resolved ones).
    def get_tcp_connection(self):
        now = time.monotonic()
        if self.socket is None or (now - self.socket_timestamp) > 180:
            self.cleanup_socket()
            # In theory, DNS should do some sort of round-robin / randomization, but better be safe
            resolved_hosts = list(self.resolve_remote_hosts())
            random.shuffle(resolved_hosts)

            for remote_ip, remote_port in resolved_hosts:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.log.info('Created TCP socket')
                self.socket.settimeout(1)
                try:
                    # Connect failure is not fatal.
                    self.socket.connect((remote_ip, remote_port))
                    self.log.info('Connected TCP socket to %s:%d', remote_ip, remote_port)
                except ConnectionError as e:
                    self.log.warning('TCP connection to %s:%d failed', remote_ip, remote_port)
                    self.cleanup_socket()
                    continue
                self.socket_timestamp = time.monotonic()
                break
        if self.socket is None:
            raise ConnectionError("No connection")
        return self.socket


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
        self.next_tick = time.monotonic() + self.tick_interval
        signal.signal(signal.SIGALRM, self.tick_handler)
        signal.setitimer(signal.ITIMER_REAL, self.tick_interval, self.tick_interval + self.tick_interval)

    def tick(self):
        now = time.monotonic()
        if now >= self.next_flush:
            if self.flush(round(time.time(), 3)):
                self.flush_interval = self.tick_interval
            else:
                self.flush_interval = min(self.flush_interval + self.flush_interval, 600)
                self.log.warning("Flush error, next in %d secs", int(self.flush_interval))
            # Occasionally, at least on macOS, kernel wakes up the process a few millis earlier
            # then scheduled (most likely scheduling we do here is introducing some small slips),
            # which triggers the condition at the top of the function and we miss a legit tick.
            # The 0.03s is a harmless margin that seems to solve the problem.
            self.next_flush = now + self.flush_interval - 0.03
        if self.self_report:
            self.take_self_report()

    def init_config(self):
        self.log = self.setup_logging(self.cfg, self.name)
        self.randomize_startup = self.cfg.get('randomize_startup', True)
        self.buffer = []
        self.buffer_limit = max(self.cfg.get('buffer_limit', 10000), 100)
        self.chunk_size = max(self.cfg.get('chunk_size', 300), 1)
        self.tick_interval = self.flush_interval = max(self.cfg['flush_interval'], 0.1)
        self.next_tick = self.next_flush = 0
        self.metadata = self.cfg.get('metadata', {})
        self.metric_postprocessor = self.cfg.get('metric_postprocessor')
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
        self.log.info("Set up")
        self.tick()
        self.setup_tick()

        if loop:
            self.loop()

    def loop(self):
        while True:
            try:
                time.sleep(60)
            except InterruptedError:
                pass

    def take_self_report(self):
        now = time.monotonic()
        if now - self.self_report_timestamp >= 60:
            self.self_report_timestamp = now
            usage = resource.getrusage(resource.RUSAGE_SELF)
            self.process_self_report(
                "bucky3",
                dict(
                    cpu=round(usage.ru_utime + usage.ru_stime, 3),
                    memory=usage.ru_maxrss,
                    uptime=round(time.monotonic() - self.init_time, 3),
                ),
                None,
                dict(name=self.name),
            )

    def process_self_report(self, bucket, stats, timestamp, metadata):
        raise NotImplementedError()

    def merge_dict(self, dst, src=None):
        if src is None:
            src = self.metadata
        if src:
            dst.update((k, v) for k, v in src.items() if k not in dst)
        return dst


class MetricsSrcProcess(MetricsProcess):
    def __init__(self, module_name, module_config, dst_pipes):
        super().__init__(module_name, module_config)
        self.dst_pipes = dst_pipes

    def init_config(self):
        super().init_config()
        self.log.info('Destination modules: ' + ', '.join(m[0] for m in self.cfg['destination_modules']))

    def buffer_metric(self, bucket, stats, timestamp, metadata):
        if metadata:
            metadata = self.merge_dict(metadata)
        else:
            metadata = self.metadata.copy()
        if 'bucket' in metadata:
            bucket = metadata['bucket']
            del metadata['bucket']
        if self.metric_postprocessor:
            postprocessed_tuple = self.metric_postprocessor(bucket, stats, timestamp, metadata)
            if postprocessed_tuple is None:
                return
            self.buffer.append(postprocessed_tuple)
        self.buffer.append((bucket, stats, timestamp, metadata))

    def process_self_report(self, bucket, stats, timestamp, metadata):
        self.buffer_metric(bucket, stats, timestamp, metadata)
        MetricsSrcProcess.flush(self, round(time.time(), 3))

    def flush(self, system_timestamp):
        if self.buffer:
            self.log.debug("Flushing %d entries from buffer", len(self.buffer))
            for chunk_start in range(0, len(self.buffer), self.chunk_size):
                chunk = self.buffer[chunk_start:chunk_start + self.chunk_size]
                for dst in self.dst_pipes:
                    dst.send(chunk)
            self.buffer = []
        return True


class MetricsDstProcess(MetricsProcess):
    def __init__(self, module_name, module_config, src_pipes):
        super().__init__(module_name, module_config)
        self.src_pipes = src_pipes

    def loop(self):
        err = 0
        while True:
            tmp = False
            for pipe in multiprocessing.connection.wait(self.src_pipes, min(self.tick_interval, 60)):
                try:
                    batch = pipe.recv()
                    self.process_batch(round(time.time(), 3), batch)
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
            if err > 10:
                self.log.error("Input(s) not ready, quitting")
                return
            elif err:
                time.sleep(1)

    def process_batch(self, recv_timestamp, batch):
        for bucket, values, timestamp, metadata in batch:
            self.process_values(recv_timestamp, bucket, values, timestamp, metadata)

    def process_self_report(self, bucket, stats, timestamp, metadata):
        self.process_values(round(time.time(), 3), bucket, stats, timestamp, self.merge_dict(metadata))

    def process_values(self, recv_timestamp, bucket, values, metric_timestamp, metadata):
        raise NotImplementedError()


class MetricsPushProcess(MetricsDstProcess, Connector):
    def __init__(self, *args, default_port=None):
        super().__init__(*args)
        self.socket = None
        self.default_port = default_port
        self.resolved_hosts = None
        self.resolved_hosts_timestamp = 0
        self.socket = None
        self.socket_timestamp = 0

    def process_batch(self, recv_timestamp, batch):
        super().process_batch(recv_timestamp, batch)
        self.tick()

    def tick(self):
        super().tick()
        self.trim_buffer()

    def trim_buffer(self):
        buffer_len = len(self.buffer)
        if buffer_len > self.buffer_limit:
            self.buffer = self.buffer[-int(self.buffer_limit / 2):]
            self.log.warning("Buffer trimmed from %d to %d entries", buffer_len, len(self.buffer))

    def push_buffer(self):
        self.log.debug('%d entries in buffer to be pushed', len(self.buffer))
        push_start, push_counter = time.monotonic(), 0
        count_limit, time_limit = self.cfg['push_count_limit'], self.cfg['push_time_limit']
        i, failed_entries = 0, []
        try:
            while i < len(self.buffer):
                if push_counter >= count_limit and (time.monotonic() - push_start) >= time_limit:
                    break
                chunk = self.buffer[i:i + self.chunk_size]
                failed_entries.extend(self.push_chunk(chunk))
                i += self.chunk_size
                push_counter += len(chunk)  # Include all entries, even the failed ones
            return push_counter > len(failed_entries)
        except (ConnectionError, socket.timeout) as e:
            self.log.exception(e)
            self.cleanup_socket()
            return False
        finally:
            self.buffer = failed_entries + self.buffer[i:]
            if self.buffer:
                self.log.warning('%d entries left over in buffer', len(self.buffer))

    def flush(self, system_timestamp):
        return self.push_buffer() if self.buffer else True
