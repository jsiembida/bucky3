

import time
import bucky.common as common


class InfluxDBClient(common.MetricsDstProcess, common.HostResolver, common.UDPConnector):
    def __init__(self, *args):
        super().__init__(*args)
        self.flush_timestamp = 0
        self.buffer = []
        self.socket = None
        self.default_port = 8086
        self.resolved_hosts = None
        self.resolved_hosts_timestamp = 0

    def tick(self):
        now = time.time()
        if len(self.buffer) > 10 or ((now - self.flush_timestamp) > 1 and len(self.buffer)):
            socket = self.get_udp_socket()
            payload = '\n'.join(self.buffer).encode()
            for ip, port in self.resolve_hosts():
                socket.sendto(payload, (ip, port))
            self.buffer = []
            self.flush_timestamp = now

    def process_metrics(self, name, values, timestamp, metadata=None):
        # https://docs.influxdata.com/influxdb/v1.2/write_protocols/line_protocol_tutorial/
        label_buf = [name]
        if metadata:
            # InfluxDB docs recommend sorting tags
            for k in sorted(metadata.keys()):
                v = metadata[k]
                # InfluxDB will drop insert with empty tags
                if v is None or v == '':
                    continue
                v = str(v).replace(' ', '')
                label_buf.append(str(k) + '=' + v)
        value_buf = []
        for k in values.keys():
            v = values[k]
            t = type(v)
            if t is int:
                value_buf.append(str(k) + '=' + str(v) + 'i')
            elif t is float or t is bool:
                value_buf.append(str(k) + '=' + str(v))
            elif t is str:
                value_buf.append(str(k) + '="' + v + '"')
        # So, the lower timestamp precisions don't seem to work with line protocol...
        line = ' '.join((','.join(label_buf), ','.join(value_buf), str(int(timestamp) * 1000000000)))
        self.buffer.append(line)
        self.tick()
