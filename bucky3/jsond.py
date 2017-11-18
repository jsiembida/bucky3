

import json
from json.decoder import JSONDecodeError
import time
import bucky3.module as module


class JsonDServer(module.MetricsSrcProcess, module.UDPConnector):
    def __init__(self, *args):
        super().__init__(*args)
        self.socket = None
        self.decoder = json.JSONDecoder()

    def flush(self, system_timestamp):
        return super().flush(system_timestamp)

    def loop(self):
        socket = self.get_udp_socket(bind=True)
        while True:
            try:
                data, addr = socket.recvfrom(65535)
                self.handle_packet(data, addr)
            except InterruptedError:
                pass

    def handle_packet(self, data, addr=None):
        try:
            recv_timestamp, data = round(time.time(), 3), data.decode('utf-8-sig')
        except UnicodeDecodeError:
            return
        for line in data.splitlines():
            line = line.strip()
            if line:
                self.handle_line(recv_timestamp, line)

    def handle_line(self, recv_timestamp, line):
        try:
            # TODO there is no protection against malicious / malformed lines
            obj, end = self.decoder.raw_decode(line)
            if end == len(line) and type(obj) is dict:
                self.handle_obj(obj)
        except JSONDecodeError as e:
            pass

    def handle_obj(self, recv_timestamp, obj):
        # Only flat objects with basic types
        for k, v in obj.items():
            if not isinstance(v, (int, float, bool, str, None)):
                return
        # Parsing ISO/RFC would be really nice, but in Python is not going to be simple
        # and fast. So let's accept only a sensible number of secs / millis from epoch.
        cust_timestamp = None
        if 'timestamp' in obj:
            cust_timestamp = obj['timestamp']
            # TODO Here we accept custom timestamps from roughly 2000 to 2060,
            # this is inconsistent with statsd module that only accepts those
            # in a very small, configurable window (i.e. +/- 10mins from now).
            if isinstance(cust_timestamp, (int, float)):
                if 1000000000 < cust_timestamp < 3000000000:  # Looks like seconds
                    pass
                elif 1000000000000 < cust_timestamp < 3000000000000:  # Looks like millis
                    cust_timestamp = cust_timestamp / 1000
                else:
                    cust_timestamp = None
            else:
                cust_timestamp = None
            del obj['timestamp']
        self.buffer_metric('metrics', obj, cust_timestamp or recv_timestamp, None)
