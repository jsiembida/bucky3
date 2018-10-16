

import json
import uuid
import zlib
import gzip
import http.client
from datetime import timezone, timedelta
import bucky3.module as module


TZ = timezone(timedelta(hours=0))


class ElasticsearchConnection(http.client.HTTPConnection):
    def __init__(self, open_socket, compression=None):
        super().__init__('elasticsearch')
        self.open_socket = open_socket
        self.compression = compression
        if compression == 'gzip':
            self.compressor = gzip.compress
        elif compression == 'deflate':
            self.compressor = zlib.compress
        else:
            self.compressor = lambda x: x

    def connect(self):
        self.sock = self.open_socket()
        try:
            self.host = self.sock.getpeername()[0]
        except OSError:
            # This can happen if in between calls something happens to the socket.
            # I.e. getpeername raises OSError when called on the closed socket,
            # we only handle ConnectionError and socket.timeout in the calling code.
            raise ConnectionError('Elasticsearch connection seems broken')

    # https://www.elastic.co/guide/en/elasticsearch/reference/5.6/docs-bulk.html
    # https://github.com/ndjson/ndjson-spec
    def bulk_upload(self, docs):
        body = ''.join(docs).encode('utf-8')
        headers = {
            # ES complains when receiving the content type with charset specified, even though
            # it does specify charset in its responses...
            # 'Content-Type': 'application/x-ndjson; charset=UTF-8'
            'Content-Type': 'application/x-ndjson'
        }
        body = self.compressor(body)
        headers['Content-Encoding'] = headers['Accept-Encoding'] = self.compression
        self.request('POST', '/_bulk', body=body, headers=headers)
        resp = self.getresponse()
        body = resp.read()
        if resp.status != 200:
            raise ConnectionError('Elasticsearch response code {}'.format(resp.status))
        if resp.headers.get('Content-Encoding') == 'deflate':
            body = zlib.decompress(body)
        elif resp.headers.get('Content-Encoding') == 'gzip':
            body = gzip.decompress(body)

        docs_rejected = 0

        try:
            body = body.decode('UTF-8')
            try:
                body = json.loads(body)
                if body.get('errors'):
                    for i in body.get('items', []):
                        doc_status = i.get('status', 200)
                        if doc_status >= 300 or doc_status < 200:
                            docs_rejected += 1
            except json.decoder.JSONDecodeError:
                raise ConnectionError('Elasticsearch response is not JSON')
        except UnicodeDecodeError:
            raise ConnectionError('Elasticsearch response is not UTF-8')

        return docs_rejected


class ElasticsearchClient(module.MetricsPushProcess, module.TCPConnector):
    def __init__(self, *args):
        super().__init__(*args, default_port=9200)

    def init_cfg(self):
        super().init_cfg()
        self.index_name = self.cfg.get('index_name')
        if self.index_name is not None:
            if not callable(self.index_name):
                static_index_name = self.index_name
                self.index_name = lambda *args: static_index_name
        self.type_name = self.cfg.get('type_name')
        self.compression = self.cfg.get('compression')
        if self.compression not in {'gzip', 'deflate'}:
            self.compression = 'identity'
        self.docs_rejected = 0

    def push_chunk(self, chunk):
        self.docs_rejected += self.elasticsearch_connection.bulk_upload(chunk)

    def flush(self, system_timestamp):
        self.elasticsearch_connection = ElasticsearchConnection(self.open_socket, self.compression)
        return super().flush(system_timestamp)

    def process_values(self, recv_timestamp, bucket, values, timestamp, metadata):
        self.merge_dict(metadata)
        self.merge_dict(values, metadata)
        timestamp = timestamp or recv_timestamp
        # ES parses the following as 'epoch_millis', see:
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
        values['timestamp'] = round(timestamp * 1000)

        if self.index_name:
            values['bucket'] = bucket
            index_name = self.index_name(bucket, values, timestamp)
        else:
            index_name = bucket
        if not index_name:
            return
        type_name = self.type_name or bucket

        # Try to produce consistent hashing, it is as consistent as json serializer inner workings.
        # I.e. serialization of floats or unicode. Should be more then enough in our case though.
        doc_str = json.dumps(values, sort_keys=True, indent=None, separators=(',', ':'))
        doc_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, doc_str))
        # TODO ES6 deprecates types, ES7 will drop them - index/type handling needs revisiting
        req = {"index": {"_index": index_name, "_type": type_name, "_id": doc_id}}
        req_str = json.dumps(req, indent=None, separators=(',', ':'))
        self.buffer_output(req_str + '\n' + doc_str + '\n')

    def produce_self_report(self):
        self_report = super().produce_self_report()
        self_report['docs_rejected'] = self.docs_rejected
        return self_report
