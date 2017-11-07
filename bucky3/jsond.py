

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
                self.buffer_metric('metrics', obj, recv_timestamp, None)
        except JSONDecodeError as e:
            pass
