

import bucky3.module as module


class InfluxDBClient(module.MetricsPushProcess, module.UDPConnector):
    def __init__(self, *args):
        super().__init__(*args, default_port=8086)

    def push_buffer(self):
        # For UDP we want to chunk it up into smaller packets.
        self.socket = self.socket or self.get_udp_socket()
        for i in range(0, len(self.buffer), self.chunk_size):
            chunk = self.buffer[i:i + self.chunk_size]
            payload = '\n'.join(chunk).encode("ascii")
            for ip, port in self.resolve_remote_hosts():
                self.socket.sendto(payload, (ip, port))

    def process_values(self, recv_timestamp, bucket, values, timestamp, metadata=None):
        # https://docs.influxdata.com/influxdb/v1.3/write_protocols/line_protocol_tutorial/
        metadata_buf = [bucket]
        if metadata:
            # InfluxDB docs recommend sorting tags
            for k in sorted(metadata.keys()):
                v = metadata[k]
                # InfluxDB will drop insert with empty tags
                if v is None or v == '':
                    continue
                v = str(v).replace(' ', '')
                metadata_buf.append(k + '=' + v.replace(',', '\\,').replace(' ', '\\ ').replace('=', '\\='))
        value_buf = []
        for k in sorted(values.keys()):
            v = values[k]
            t = type(v)
            if t is float or t is int or t is bool:
                value_buf.append(str(k) + '=' + str(v))
            elif t is str:
                value_buf.append(str(k) + '="' + v.replace('"', r'\"') + '"')
        line = ' '.join((','.join(metadata_buf), ','.join(value_buf)))
        if timestamp is not None:
            # So, the lower timestamp precisions don't seem to work with line protocol...
            line += ' ' + str(int(timestamp * 1000000000))
        self.buffer.append(line)

    def process_value(self, recv_timestamp, bucket, value, timestamp, metadata=None):
        self.process_values(recv_timestamp, bucket, {'value': value}, timestamp, metadata)
