

import time
import threading
import http.server
import bucky.module as module


class PrometheusExporter(module.MetricsDstProcess):
    def __init__(self, *args):
        super().__init__(*args)
        self.buffer = {}

    def start_http_server(self, host, port, path):
        def do_GET(req):
            if req.path.strip('/') != path:
                req.send_response(404)
                req.send_header("Content-type", "text/plain")
                req.end_headers()
            else:
                req.send_response(200)
                req.send_header("Content-Type", "text/plain; version=0.0.4")
                req.end_headers()
                response = ''.join(self.get_or_render_line(k) for k in self.buffer.keys())
                req.wfile.write(response.encode())

        def log_message(req, format, *args):
            self.log.info(format, *args)

        handler = type('PrometheusHandler', (http.server.BaseHTTPRequestHandler,),
                       {'do_GET': do_GET, 'log_message': log_message})
        http_server = http.server.HTTPServer((host, port), handler)
        http_thread = threading.Thread(target=lambda: http_server.serve_forever())
        http_thread.start()
        self.log.info("Started server at http://%s:%d/%s", host, port, path)

    def get_or_render_line(self, k):
        timestamp, value, line = self.buffer[k]
        if not line:
            # https://prometheus.io/docs/instrumenting/exposition_formats/
            bucket, metadata = k[0], k[1:]
            metadata_str = ','.join(str(k) + '="' + str(v) + '"' for k, v in metadata)
            # Lines MUST end with \n (not \r\n), the last line MUST also end with \n
            # Otherwise, Prometheus will reject the whole scrape!
            line = bucket + '{' + metadata_str + '} ' + str(value) + ' ' + str(int(timestamp * 1000)) + '\n'
            self.buffer[k] = timestamp, value, line
        return line

    def loop(self):
        host = self.cfg.get("local_host", "127.0.0.1")
        port = self.cfg.get("local_port", 9090)
        path = self.cfg.get("http_path", "metrics")
        self.start_http_server(host, port, path)
        super().loop()

    def flush(self):
        now = time.time()
        timeout = self.cfg['values_timeout']
        old_keys = [k for k, (timestamp, v, l) in self.buffer.items() if (now - timestamp) > timeout]
        for k in old_keys:
            del self.buffer[k]
        return True

    def process_values(self, bucket, values, timestamp, metadata=None):
        for k, v in values.items():
            metadata_dict = dict(value=k)
            if metadata:
                metadata_dict.update(metadata)
            metadata_tuple = (bucket,) + tuple((k, metadata_dict[k]) for k in sorted(metadata_dict.keys()))
            # The None below will get lazily rendered during HTTP req
            self.buffer[metadata_tuple] = timestamp, v, None
