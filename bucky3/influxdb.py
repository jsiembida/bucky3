

import bucky3.module as module


class InfluxDBClient(module.MetricsPushProcess, module.UDPConnector):
    def __init__(self, *args):
        super().__init__(*args, default_port=8086)

    def push_chunk(self, chunk):
        payload = '\n'.join(chunk).encode("ascii")
        for ip, port in self.resolve_remote_hosts():
            self.sock.sendto(payload, (ip, port))
        return []

    def flush(self, system_timestamp):
        self.open_socket()
        return super().flush(system_timestamp)

    def process_values(self, recv_timestamp, bucket, values, timestamp, metadata):
        # https://docs.influxdata.com/influxdb/v1.3/write_protocols/line_protocol_tutorial/
        metadata_buf = [bucket]
        # InfluxDB docs recommend sorting tags
        for k in sorted(metadata.keys()):
            v = metadata[k]
            # InfluxDB will drop insert with empty tags
            if v is None or v == '':
                continue
            metadata_buf.append(k + '=' + str(v).replace(',', '\\,').replace(' ', '\\ ').replace('=', '\\='))
        value_buf = []
        for k in sorted(values.keys()):
            v = values[k]
            if isinstance(v, (float, int, bool)):
                value_buf.append(str(k) + '=' + str(v))
            elif isinstance(v, str):
                value_buf.append(str(k) + '="' + v.replace('"', r'\"') + '"')
        line = ' '.join((','.join(metadata_buf), ','.join(value_buf)))
        if timestamp is not None:
            # So, the lower timestamp precisions don't seem to work with line protocol...
            line += ' ' + str(int(timestamp * 1000000000))
        self.buffer_output(line)
