

import time
import threading
import http.server
import bucky.cfg as cfg
import bucky.common as common


class PrometheusExporter(common.MetricsDstProcess):
    def __init__(self, *args):
        super().__init__(*args)
        self.flush_timestamp = 0
        self.buffer = {}
        self.http_host = None
        self.http_port = None
        self.http_thread = None
        self.http_server = None

    def run(self):
        super().run()

    def start_http_server(self):
        if self.http_port != cfg.port or self.http_host != cfg.host:
            if self.http_server:
                cfg.log.info("Stopping server running at %s:%d", self.http_host, self.http_port)
                self.http_server.shutdown()
                self.http_thread.join()
                cfg.log.debug("Server at %s:%d stopped", self.http_host, self.http_port)
            self.http_port = self.http_host = self.http_server = self.http_thread = None

        def do_GET(req):
            if req.path.strip('/') != cfg.path:
                req.send_response(404)
                req.send_header("Content-type", "text/plain")
                req.end_headers()
            else:
                req.send_response(200)
                req.send_header("Content-Type", "text/plain; version=0.0.4")
                req.end_headers()
                response = ''.join(self.get_or_render_line(k) for k in self.buffer.keys())
                req.wfile.write(response.encode())

        if not self.http_server:
            handler = type('PrometheusHandler', (http.server.BaseHTTPRequestHandler,), {'do_GET': do_GET})
            cfg.log.debug("Starting server at %s:%d", cfg.host, cfg.port)
            # TODO make the server use the same logging as logger
            self.http_server = http.server.HTTPServer((cfg.host, cfg.port), handler)
            self.http_thread = threading.Thread(target=lambda: self.http_server.serve_forever())
            self.http_thread.start()
            cfg.log.info("Started server at %s:%d", cfg.host, cfg.port)
            self.http_port = cfg.port
            self.http_host = cfg.host

    def get_or_render_line(self, k):
        timestamp, value, line = self.buffer[k]
        if not line:
            # https://prometheus.io/docs/instrumenting/exposition_formats/
            name, metadata = k[0], k[1:]
            metadata_str = ','.join(str(k) + '="' + str(v) + '"' for k, v in metadata)
            # Lines MUST end with \n (not \r\n), the last line MUST also end with \n
            # Otherwise, Prometheus will reject the whole scrape!
            line = name + '{' + metadata_str + '} ' + str(value) + ' ' + str(int(timestamp) * 1000) + '\n'
            self.buffer[k] = timestamp, value, line
        return line

    def tick(self):
        now = time.time()
        if (now - self.flush_timestamp) > cfg.interval:
            old_keys = [k for k, (timestamp, value, line) in self.buffer.items() if (now - timestamp) > cfg.timeout]
            for k in old_keys:
                del self.buffer[k]
            self.flush_timestamp = now
            self.start_http_server()
        return True

    def process_metrics(self, name, values, timestamp, metadata=None):
        for k, v in values.items():
            metadata_dict = dict(value=k)
            if metadata:
                metadata_dict.update(metadata)
            metadata_tuple = (name,) + tuple((k, metadata_dict[k]) for k in sorted(metadata_dict.keys()))
            self.buffer[metadata_tuple] = timestamp, v, None
        self.tick()
