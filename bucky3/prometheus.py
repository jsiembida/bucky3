

import gzip
import http.server
import bucky3.module as module


class PrometheusExporter(module.MetricsDstProcess, module.HostResolver):
    def __init__(self, *args):
        super().__init__(*args)
        self.http_requests = 0

    def init_cfg(self):
        super().init_cfg()
        self.buffer = {}
        self.compression = self.cfg.get('compression')
        if self.compression != 'gzip':
            self.compression = None

    def start_http_server(self, ip, port, path):
        def do_GET(req):
            if req.path.strip('/') != path:
                req.send_response(404)
                req.send_header("Content-type", "text/plain")
                req.end_headers()
            else:
                req.send_response(200)
                req.send_header("Content-Type", "text/plain; version=0.0.4")
                write, flush, close = req.wfile.write, req.wfile.flush, None
                if self.compression == 'gzip' and 'gzip' in req.headers.get('Accept-Encoding', ''):
                    req.send_header('Content-Encoding', self.compression)
                    req.end_headers()
                    gzip_stream = gzip.GzipFile(filename='', mode='wb', fileobj=req.wfile)
                    write, flush, close = gzip_stream.write, gzip_stream.flush, gzip_stream.close
                else:
                    req.end_headers()
                for chunk in self.get_chunks():
                    write(chunk.encode("utf-8"))
                    flush()
                if close:
                    close()
            self.http_requests += 1

        def log_message(req, format, *args):
            self.log.debug(format, *args)

        def handle(req):
            try:
                http.server.BaseHTTPRequestHandler.handle(req)
            except ConnectionResetError:
                pass

        handler = type(
            'PrometheusHandler',
            (http.server.BaseHTTPRequestHandler,),
            {
                'handle': handle,
                'do_GET': do_GET,
                'log_message': log_message,
                # With the default wbufsize=0 the _SocketWriter() is used in StreamRequestHandler
                # and that causes payload corruption when request is being interrupted by alarm.
                # With the wbufsize>0 the buffered socket IO is used and that seems to work fine.
                # Which is weird because in recent Pythons all interrupted calls should restart.
                'wbufsize': 256*1024,
                'timeout': self.socket_timeout
            }
        )
        http_server = http.server.HTTPServer((ip, port), handler)
        self.start_thread('HttpServerThread', http_server.serve_forever)
        self.log.info("Started server at http://%s:%d/%s", ip, port, path)

    def get_line(self, bucket, value, metadata, timestamp):
        # https://prometheus.io/docs/instrumenting/exposition_formats/
        metadata_str = ','.join(
            k + '="' + metadata[k].replace('\\', '\\\\').replace('"', '\\"') + '"' for k in sorted(metadata)
        )
        # metric_str is a canonical part of the metric, used as a key, too
        metric_str = bucket
        if metadata_str:
            metric_str += '{' + metadata_str + '}'
        line_str = metric_str + ' ' + str(value)
        if timestamp is not None:
            line_str += ' ' + str(int(timestamp * 1000))
        # Lines MUST end with \n (not \r\n), the last line MUST also end with \n
        # Otherwise, Prometheus will reject the whole scrape!
        line_str += '\n'
        return metric_str, line_str

    def get_chunks(self):
        with self.buffer_lock:
            buffer = tuple(metric_line for recv_timestamp, metric_line in self.buffer.values())
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
        with self.buffer_lock:
            old_keys = [
                k for k, (recv_timestamp, metric_line) in self.buffer.items()
                if (system_timestamp - recv_timestamp) > timeout
            ]
            for k in old_keys:
                del self.buffer[k]
            return True

    def produce_self_report(self):
        self_report = super().produce_self_report()
        self_report['metrics_received'] = self.metrics_received
        self_report['http_requests'] = self.http_requests
        return self_report

    def process_values(self, recv_timestamp, bucket, values, metrics_timestamp, metadata):
        for k, v in values.items():
            if isinstance(v, bool):
                v = int(v)
            if isinstance(v, (int, float)):
                metadata['value'] = k
                metric_str, line_str = self.get_line(bucket, v, metadata, metrics_timestamp)
                with self.buffer_lock:
                    self.buffer[metric_str] = recv_timestamp, line_str
