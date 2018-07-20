

import io
import sys
import time
import socket
import signal
import random
import logging
import resource
import threading
import multiprocessing
import multiprocessing.connection


def cached_with_timeout(timeout, allow_none=False):
    def decorator(func):
        timestamp, value = 0, None

        def wrapper(*args, **kwargs):
            nonlocal timestamp, value
            now = time.monotonic()
            if (now - timestamp > timeout) or (value is None and not allow_none):
                timestamp, value = now, func(*args, **kwargs)
            return value

        return wrapper

    return decorator


class Logger:
    def init_log(self, cfg, module_name=None):
        # Reinit those to avoid races on the underlying streams
        sys.stdout = io.TextIOWrapper(io.FileIO(1, mode='wb', closefd=False))
        sys.stderr = io.TextIOWrapper(io.FileIO(2, mode='wb', closefd=False))
        root = logging.getLogger(module_name)
        for h in list(root.handlers):
            root.removeHandler(h)
        root.setLevel(cfg.get('log_level', 'INFO'))
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            cfg.get('log_format', "[%(asctime)-15s][%(levelname)s] %(name)s(%(threadName)s@%(process)d) - %(message)s")
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
        try:
            hostname, aliaslist, ipaddrlist = socket.gethostbyname_ex(host)
            return {(ip, port) for ip in ipaddrlist}
        except socket.gaierror:
            return set()

    def resolve_host(self, host, default_port):
        for ip, port in self.parse_address(host, default_port):
            self.log.debug("Resolved %s as %s:%d", host, ip, port)
            yield ip, port

    def resolve_local_host(self, default_port=0):
        local_host = self.cfg.get('local_host', '0.0.0.0')
        resolved_hosts = tuple(self.resolve_host(local_host, default_port))
        if resolved_hosts:
            return random.choice(resolved_hosts)
        # Resolution failure for local host should be fatal (most likely misconfiguration)
        raise ValueError("Could not resolve local host " + str(local_host))

    # DNS resolution interval could be parametrized, but it seems a bit involved.
    # The hardcoded 180s seems to be reasonable.
    @cached_with_timeout(timeout=180)
    def resolve_remote_hosts(self):
        resolved_hosts = set()
        for host in self.cfg['remote_hosts']:
            resolved_hosts.update(self.resolve_host(host, self.default_port))
        # Resolution failure for remote hosts should not be fatal (i.e. temporary DNS issue)
        return resolved_hosts


class Connector:
    def close_socket(self):
        if self.sock:
            self.sock.close()
            self.log.debug('Closed socket')
        self.sock = None


class UDPConnector(Connector, HostResolver):
    def open_socket(self, bind=False):
        # UDP sockets don't need the elaborate recycling TCP sockets do
        if self.sock is None:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            if self.socket_timeout is not None:
                self.sock.settimeout(self.socket_timeout)
            self.log.info('Created UDP socket')
            if bind:
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if hasattr(socket, 'SO_REUSEPORT'):
                    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                ip, port = self.resolve_local_host()
                self.sock.bind((ip, port))
                self.log.info("Bound UDP socket %s:%d", ip, port)
        return self.sock


class TCPConnector(Connector, HostResolver):
    # To provide load balancing, when pushing via TCP, we reopen the connection
    # at intervals (using a random host from the pool of resolved ones).
    @cached_with_timeout(timeout=180)
    def open_socket(self):
        self.close_socket()

        # TODO use socket.create_connection instead?
        resolved_hosts = list(self.resolve_remote_hosts())
        if resolved_hosts:
            # DNS does some sort of round-robin / randomization, but this way we
            # reshuffle on every attempt rather than only on each DNS query.
            # This seems to provide more randomness / spread in connections.
            random.shuffle(resolved_hosts)

            for remote_ip, remote_port in resolved_hosts:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if self.socket_timeout is not None:
                    self.sock.settimeout(self.socket_timeout)
                self.log.info('Created TCP socket')
                try:
                    # Connect failure is not fatal.
                    self.sock.connect((remote_ip, remote_port))
                    self.log.info('Connected TCP socket to %s:%d', remote_ip, remote_port)
                except (ConnectionError, socket.timeout):
                    self.log.warning('TCP connection to %s:%d failed', remote_ip, remote_port)
                    self.close_socket()
                    continue
                break
        if self.sock is None:
            raise ConnectionError("No connection could be found")
        return self.sock


class MetricsProcess(multiprocessing.Process, Logger):
    def __init__(self, module_name, module_config):
        super().__init__(name=module_name, daemon=True)
        self.cfg = module_config
        self.flush_errors = 0

    def tick(self):
        self.log.debug("Flush")
        if self.flush(round(time.time(), 3)):
            self.flush_interval = self.tick_interval
        else:
            self.flush_interval = min(self.flush_interval + self.flush_interval, self.max_flush_interval)
            self.log.warning("Flush error, next in %d secs", int(self.flush_interval))
            self.flush_errors += 1

    def init_cfg(self):
        self.log = self.init_log(self.cfg, self.name)
        self.randomize_startup = self.cfg.get('randomize_startup', True)
        self.buffer = []
        self.buffer_lock = threading.Lock()
        self.buffer_limit = max(self.cfg.get('buffer_limit', 10000), 100)
        self.socket_timeout = self.cfg.get('socket_timeout', None)
        if self.socket_timeout is not None:
            self.socket_timeout = max(self.socket_timeout, 1)
        self.chunk_size = max(self.cfg.get('chunk_size', 300), 1)
        self.tick_interval = self.flush_interval = max(self.cfg['flush_interval'], 1)
        self.max_flush_interval = max(self.flush_interval, self.cfg.get('max_flush_interval', 600))
        self.next_tick = self.next_flush = 0
        self.metadata = self.cfg.get('metadata', {})
        self.metric_postprocessor = self.cfg.get('metric_postprocessor')
        self.add_timestamps = self.cfg.get('add_timestamps', False)
        self.self_report = self.cfg.get('self_report', False)
        self.init_timestamp = time.monotonic()
        self.threads = []

    def run(self):
        def termination_handler(signal_number, stack_frame):
            self.log.info("Received signal %d, exiting", signal_number)
            sys.exit(0)

        signal.signal(signal.SIGINT, termination_handler)
        signal.signal(signal.SIGTERM, termination_handler)
        signal.signal(signal.SIGHUP, termination_handler)

        self.init_cfg()
        self.log.info("Set up")
        if self.randomize_startup and self.tick_interval > 3:
            # If randomization is configured (it's default) do it asap
            time.sleep(random.randint(0, min(self.tick_interval - 1, 15)))
        self.loop()

    def ended_threads(self):
        ended = False
        for thread in (thread for thread in self.threads if not thread.is_alive()):
            self.log.error("Thread %s exited", thread.name)
            ended = True
        return ended

    def loop(self):
        self.next_tick = time.monotonic()
        while True:
            try:
                self.log.debug("Tick")
                now = time.monotonic()
                if now >= self.next_flush:
                    self.tick()
                    # Occasionally, at least on macOS, kernel wakes up the process a few millis earlier
                    # then scheduled (most likely scheduling we do here is introducing some small slips),
                    # which triggers the condition at the top of the function and we miss a legit tick.
                    # The 0.03s is a harmless margin that seems to solve the problem.
                    self.next_flush = now + self.flush_interval - 0.03
                if self.ended_threads():
                    self.log.error("Aborting")
                    sys.exit(1)
                if self.self_report:
                    self.take_self_report()
                now = time.monotonic()
                while now + 0.3 >= self.next_tick:
                    self.next_tick += self.tick_interval
                sleep_duration = self.next_tick - now
                if sleep_duration > 4:
                    sleep_duration = 3
                time.sleep(sleep_duration)
            except InterruptedError:
                pass

    def produce_self_report(self):
        now = time.monotonic()
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return {
            'cpu': round(usage.ru_utime + usage.ru_stime, 3),
            'memory': usage.ru_maxrss,
            'uptime': round(now - self.init_timestamp, 3),
            'flush_errors': self.flush_errors,
        }

    @cached_with_timeout(timeout=60, allow_none=True)
    def take_self_report(self):
        # Source modules will push their self reported metrics to their respective destination modules.
        # But destination modules only expose their metrics to what consumes their output.
        self.process_self_report(
            "bucky3",
            self.produce_self_report(),
            None,
            {'name': self.name},
        )

    def merge_dict(self, dst, src=None):
        if src is None:
            src = self.metadata
        if src:
            dst.update((k, v) for k, v in src.items() if k not in dst)
        return dst

    def start_thread(self, name, target):
        thread = threading.Thread(name=name, target=target, daemon=True)
        thread.start()
        self.threads.append(thread)


class MetricsSrcProcess(MetricsProcess):
    def __init__(self, module_name, module_config, dst_pipes):
        super().__init__(module_name, module_config)
        self.dst_pipes = dst_pipes
        self.metrics_produced = 0
        self.metrics_dropped = 0

    def init_cfg(self):
        super().init_cfg()
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
                self.metrics_dropped += 1
            else:
                with self.buffer_lock:
                    self.buffer.append(postprocessed_tuple)
                    self.metrics_produced += 1
        else:
            with self.buffer_lock:
                self.buffer.append((bucket, stats, timestamp, metadata))
                self.metrics_produced += 1

    def produce_self_report(self):
        self_report = super().produce_self_report()
        self_report['metrics_produced'] = self.metrics_produced
        self_report['metrics_dropped'] = self.metrics_dropped
        return self_report

    def process_self_report(self, bucket, stats, timestamp, metadata):
        self.buffer_metric(bucket, stats, timestamp, metadata)

    def flush(self, system_timestamp):
        while self.buffer:
            with self.buffer_lock:
                chunk = self.buffer[0:self.chunk_size]
                if not chunk:
                    break
                # TODO this doesn't look sound, if sending to dst pipes fails for a reason later on, we lose the chunk
                del self.buffer[0:self.chunk_size]
            self.log.debug("Flushing %d entries from buffer", len(chunk))
            for dst in self.dst_pipes:
                dst.send(chunk)
        return True


class MetricsDstProcess(MetricsProcess):
    def __init__(self, module_name, module_config, src_pipes):
        super().__init__(module_name, module_config)
        self.src_pipes = src_pipes
        self.metrics_received = 0

    def read_loop(self):
        err = 0
        while True:
            tmp = False
            for pipe in multiprocessing.connection.wait(self.src_pipes):
                try:
                    self.process_batch(round(time.time(), 3), pipe.recv())
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
            if err:
                time.sleep(1)

    def loop(self):
        self.start_thread('SrcReadThread', self.read_loop)
        super().loop()

    def process_batch(self, recv_timestamp, batch):
        for bucket, values, timestamp, metadata in batch:
            self.process_values(recv_timestamp, bucket, values, timestamp, metadata)
            self.metrics_received += 1

    def process_self_report(self, bucket, stats, timestamp, metadata):
        self.process_values(round(time.time(), 3), bucket, stats, timestamp, self.merge_dict(metadata))


class MetricsPushProcess(MetricsDstProcess, Connector):
    def __init__(self, *args, default_port=None):
        super().__init__(*args)
        self.sock = None
        self.default_port = default_port
        self.metrics_sent = 0
        self.metrics_rejected = 0
        self.metrics_dropped = 0
        self.connection_errors = 0

    def init_cfg(self):
        super().init_cfg()
        self.push_count_limit = self.cfg.get('push_count_limit', self.buffer_limit)
        self.push_time_limit = self.cfg.get('push_time_limit', max(self.tick_interval / 3, 0.1))

    def tick(self):
        super().tick()
        self.trim_buffer()

    def trim_buffer(self):
        with self.buffer_lock:
            buffer_len = len(self.buffer)
            if buffer_len > self.buffer_limit:
                self.buffer = self.buffer[-int(self.buffer_limit / 2):]
                self.log.warning("Buffer trimmed from %d to %d entries", buffer_len, len(self.buffer))
                self.metrics_dropped += (buffer_len - len(self.buffer))

    def buffer_output(self, data):
        with self.buffer_lock:
            self.buffer.append(data)

    def produce_self_report(self):
        self_report = super().produce_self_report()
        self_report['metrics_received'] = self.metrics_received
        self_report['metrics_sent'] = self.metrics_sent
        self_report['metrics_rejected'] = self.metrics_rejected
        self_report['connection_errors'] = self.connection_errors
        return self_report

    def flush(self, system_timestamp):
        if not self.buffer:
            return True
        self.log.debug('%d entries in buffer to be pushed', len(self.buffer))
        push_start, push_counter, rejected_entries = time.monotonic(), 0, []
        try:
            while self.buffer:
                if push_counter >= self.push_count_limit:
                    break
                if time.monotonic() - push_start >= self.push_time_limit:
                    break
                chunk = self.buffer[:self.chunk_size]
                chunk_len = len(chunk)
                # TODO we don't use the rejected metrics logic anywhere, remove it? Make it work?
                rejected_chunk = self.push_chunk(chunk)
                rejected_entries.extend(rejected_chunk)
                self.metrics_sent += chunk_len - len(rejected_chunk)
                self.metrics_rejected += len(rejected_chunk)
                with self.buffer_lock:
                    del self.buffer[:chunk_len]
                push_counter += chunk_len  # Include all entries, even the failed ones
            # If we manage to push something then report success. Ok?
            return push_counter > len(rejected_entries)
        except (ConnectionError, socket.timeout) as e:
            self.log.exception(e)
            self.close_socket()
            self.connection_errors += 1
            return False
        finally:
            if rejected_entries:
                with self.buffer_lock:
                    self.buffer = rejected_entries + self.buffer
            if self.buffer:
                self.log.warning('%d entries left over in buffer', len(self.buffer))
