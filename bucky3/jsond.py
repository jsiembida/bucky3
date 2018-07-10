

import json
import time
import socket
import zlib
import gzip
import bucky3.module as module


class JsonDServer(module.MetricsSrcProcess, module.UDPConnector):
    def __init__(self, *args):
        super().__init__(*args)
        self.sock = None
        self.decoder = json.JSONDecoder()

    def init_cfg(self):
        super().init_cfg()
        self.timestamp_window = self.cfg.get('timestamp_window', 600)

    def read_loop(self):
        sock = self.open_socket(bind=True)
        while True:
            try:
                data, addr = sock.recvfrom(65535)
                try:
                    data = zlib.decompress(data)
                except zlib.error:
                    try:
                        data = gzip.decompress(data)
                    except OSError:
                        pass
                self.handle_packet(data, addr)
            except (InterruptedError, socket.timeout):
                pass

    def loop(self):
        self.start_thread('UdpReadThread', self.read_loop)
        super().loop()

    def handle_packet(self, data, addr=None):
        try:
            recv_timestamp, data = round(time.time(), 3), data.decode('utf-8')
        except UnicodeDecodeError:
            return
        # http://ndjson.org/
        for line in data.splitlines():
            line = line.strip()
            if line:
                self.handle_line(recv_timestamp, line)

    def handle_line(self, recv_timestamp, line):
        try:
            # TODO there is no protection against malicious / malformed lines
            obj, end = self.decoder.raw_decode(line)
            if end == len(line) and isinstance(obj, dict):
                self.handle_obj(recv_timestamp, obj)
        except ValueError:
            return

    def handle_obj(self, recv_timestamp, obj):
        # Only flat objects with basic types
        for k, v in obj.items():
            if not isinstance(v, (int, float, bool, str)) and v is not None:
                return
        # Parsing ISO/RFC would be really nice, but in Python is not going to be simple
        # and fast. So let's accept only a sensible number of secs / millis from epoch.
        cust_timestamp = None
        if 'timestamp' in obj:
            cust_timestamp = float(obj['timestamp'])
            # Assume millis not secs if the timestamp >= 2^31
            if cust_timestamp > 2147483647:
                cust_timestamp /= 1000
            if abs(recv_timestamp - cust_timestamp) > self.timestamp_window:
                return
            del obj['timestamp']
        self.buffer_metric('metrics', obj, cust_timestamp or recv_timestamp, None)
