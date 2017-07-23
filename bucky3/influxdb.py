

import bucky3.module as module


class InfluxDBClient(module.MetricsPushProcess, module.UDPConnector):
    def __init__(self, *args):
        super().__init__(*args, default_port=8086)

    def push_buffer(self):
        # For UDP we want to chunk it up into smaller packets.
        self.socket = self.socket or self.get_udp_socket()
        chunk_size = 5
        for i in range(0, len(self.buffer), chunk_size):
            chunk = self.buffer[i:i + chunk_size]
            payload = '\n'.join(chunk).encode("ascii")
            for ip, port in self.resolve_hosts():
                self.socket.sendto(payload, (ip, port))

    def process_values(self, bucket, values, timestamp, metadata=None):
        # https://docs.influxdata.com/influxdb/v1.2/write_protocols/line_protocol_tutorial/
        label_buf = [bucket]
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
        for k in sorted(values.keys()):
            v = values[k]
            t = type(v)
            if t is int:
                value_buf.append(str(k) + '=' + str(v) + 'i')
            elif t is float or t is bool:
                value_buf.append(str(k) + '=' + str(v))
            elif t is str:
                value_buf.append(str(k) + '="' + v + '"')
        # So, the lower timestamp precisions don't seem to work with line protocol...
        line = ' '.join((','.join(label_buf), ','.join(value_buf), str(int(timestamp * 1000000000))))
        self.buffer.append(line)

    def process_value(self, bucket, value, timestamp, metadata=None):
        self.process_values(bucket, {'value': value}, timestamp, metadata)
