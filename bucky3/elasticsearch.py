

import json
import uuid
import zlib
import gzip
import http.client
from datetime import datetime, timezone, timedelta
import bucky3.module as module


TZ = timezone(timedelta(hours=0))


class ElasticsearchConnection(http.client.HTTPConnection):
    def __init__(self, socket_getter, compression=None):
        super().__init__('elasticsearch')
        self.socket_getter = socket_getter
        self.compression = compression
        self.compressor = dict(gzip=gzip.compress, deflate=zlib.compress).get(compression, lambda x: x)

    def connect(self):
        self.sock = self.socket_getter()
        self.host = self.sock.getpeername()

    # https://www.elastic.co/guide/en/elasticsearch/reference/5.6/docs-bulk.html
    # https://github.com/ndjson/ndjson-spec
    def bulk_upload(self, docs):
        buffer = []
        for req_str, doc_str in docs:
            buffer.append(req_str)
            buffer.append('\n')
            buffer.append(doc_str)
            buffer.append('\n')
        body = ''.join(buffer).encode('utf-8')
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
        if resp.status != 200:
            raise ConnectionError('Elasticsearch error code {}'.format(resp.status))
        body = resp.read()
        # Needed? Is HTTP code not enough?
        # if resp.headers['Content-Encoding'] == 'deflate':
        #     body = zlib.decompress(body)
        # elif resp.headers['Content-Encoding'] == 'gzip':
        #     body = gzip.decompress(body)
        # return json.loads(body.decode('utf-8'))


class ElasticsearchClient(module.MetricsPushProcess, module.TCPConnector):
    def __init__(self, *args):
        super().__init__(*args, default_port=9200)

    def init_config(self):
        super().init_config()
        self.index_name = self.cfg.get('index_name')
        self.type_name = self.cfg.get('type_name')
        self.compression = self.cfg.get('compression')
        if self.compression not in {'gzip', 'deflate'}:
            self.compression = 'identity'

    def push_chunk(self, chunk):
        self.elasticsearch_connection.bulk_upload(chunk)
        return []

    def push_buffer(self):
        self.elasticsearch_connection = ElasticsearchConnection(self.get_tcp_connection, self.compression)
        return super().push_buffer()

    def process_values(self, recv_timestamp, bucket, values, timestamp, metadata):
        self.merge_dict(metadata)
        self.merge_dict(values, metadata)
        timestamp = timestamp or recv_timestamp
        # ES can be configured otherwise, but by default it only takes a space separated string
        # with millisecond precision without a TZ (i.e. '2017-11-08 11:04:48.102').
        # Python's datetime.isoformat on the other hand, only produces microsecond precision
        # (optional parameter to control it was introduced in Python 3.6) and has the edge case
        # where it is skipping the fraction part altogether if microseconds==0.
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
        # https://docs.python.org/3/library/datetime.html#datetime.datetime.isoformat
        timestamp = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        values['timestamp'] = timestamp

        if self.index_name:
            values['bucket'] = bucket
            index_name = self.index_name
        else:
            index_name = bucket
        type_name = self.type_name or bucket

        # Try to produce consistent hashing, it is as consistent as json serializer inner workings.
        # I.e. serialization of floats or unicode. Should be more then enough in our case though.
        doc_str = json.dumps(values, sort_keys=True, indent=None, separators=(',', ':'))
        doc_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, doc_str))
        # TODO ES6 deprecates types, ES7 will drop them - index/type handling needs revisiting
        req = {"index": {"_index": index_name, "_type": type_name, "_id": doc_id}}
        req_str = json.dumps(req, indent=None, separators=(',', ':'))
        with self.buffer_lock:
            self.buffer.append((req_str, doc_str))
