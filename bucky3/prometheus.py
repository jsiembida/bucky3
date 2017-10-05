

import threading
import http.server
import bucky3.module as module


class PrometheusExporter(module.MetricsDstProcess, module.HostResolver):
    def __init__(self, *args):
        super().__init__(*args)

    def init_config(self):
        super().init_config()
        self.buffer = {}

    def start_http_server(self, ip, port, path):
        def do_GET(req):
            if req.path.strip('/') != path:
                req.send_response(404)
                req.send_header("Content-type", "text/plain")
                req.end_headers()
            else:
                req.send_response(200)
                req.send_header("Content-Type", "text/plain; version=0.0.4")
                req.end_headers()
                for chunk in self.get_chunks():
                    req.wfile.write(chunk.encode("ascii"))
                    req.wfile.flush()

        def log_message(req, format, *args):
            self.log.info(format, *args)

        handler = type(
            'PrometheusHandler',
            (http.server.BaseHTTPRequestHandler,),
            {
                'do_GET': do_GET,
                'log_message': log_message,
                'timeout': 3
            }
        )
        http_server = http.server.HTTPServer((ip, port), handler)
        http_thread = threading.Thread(target=lambda: http_server.serve_forever())
        http_thread.start()
        self.log.info("Started server at http://%s:%d/%s", ip, port, path)

    def get_line(self, k):
        tmp = self.buffer.get(k)
        if not tmp:
            return ''
        timestamp, value, line = tmp
        if not line:
            # https://prometheus.io/docs/instrumenting/exposition_formats/
            bucket, metadata = k[0], k[1:]
            if metadata:
                metadata_str = ','.join(str(k) + '="' + str(v) + '"' for k, v in metadata)
                # Lines MUST end with \n (not \r\n), the last line MUST also end with \n
                # Otherwise, Prometheus will reject the whole scrape!
                line = bucket + '{' + metadata_str + '} ' + str(value) + ' ' + str(int(timestamp * 1000)) + '\n'
            else:
                line = bucket + ' ' + str(value) + ' ' + str(int(timestamp * 1000)) + '\n'
            self.buffer[k] = timestamp, value, line
        return line

    def get_chunks(self):
        buffer = tuple(self.get_line(k) for k in tuple(self.buffer.keys()))
        for chunk_start in range(0, len(buffer), self.chunk_size):
            chunk = buffer[chunk_start:chunk_start + self.chunk_size]
            yield ''.join(chunk)

    def get_page(self):
        return ''.join(self.get_chunks())

    def loop(self):
        ip, port = self.resolve_local_host(9103)
        path = self.cfg.get("http_path", "metrics")
        self.start_http_server(ip, port, path)
        super().loop()

    def flush(self, system_timestamp):
        timeout = self.cfg['values_timeout']
        old_keys = [k for k, (timestamp, v, l) in self.buffer.items() if (system_timestamp - timestamp) > timeout]
        for k in old_keys:
            del self.buffer[k]
        return True

    def process_values(self, bucket, values, timestamp, metadata=None):
        for k, v in values.items():
            if metadata:
                metadata_dict = metadata.copy()
                metadata_dict.update(value=k)
            else:
                metadata_dict = dict(value=k)
            self.process_value(bucket, v, timestamp, metadata_dict)

    def process_value(self, bucket, value, timestamp, metadata=None):
        if metadata:
            metadata_tuple = (bucket,) + tuple((k, metadata[k]) for k in sorted(metadata.keys()))
        else:
            metadata_tuple = (bucket,)
        # The None below will get lazily rendered during HTTP req
        self.buffer[metadata_tuple] = timestamp, value, None
