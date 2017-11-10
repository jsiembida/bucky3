

import json
import uuid
import http.client
from datetime import datetime, timezone, timedelta
import bucky3.module as module


TZ = timezone(timedelta(hours=0))


class ElasticsearchConnection(http.client.HTTPConnection):
    def __init__(self, socket_getter, es_host, es_type=None):
        super().__init__(es_host)
        self.es_type = es_type
        self.socket_getter = socket_getter

    def connect(self):
        self.sock = self.socket_getter()

    # https://www.elastic.co/guide/en/elasticsearch/reference/5.6/docs-bulk.html
    # https://github.com/ndjson/ndjson-spec
    def bulk_upload(self, docs):
        buffer = []
        for bucket, timestamp, doc in docs:
            # ES can be configured otherwise, but by default it has an interesting approach to
            # "we support ISO format". It only takes a space separated string with millisecond
            # precision without TZ (i.e. '2017-11-08 11:04:48.102'), everything else seems to fail.
            # Python's datetime.isoformat on the other hand, only produces microsecond precision
            # (optional parameter to control it was introduced in Python 3.6) and has the edge case
            # where it is skipping the fraction part altogether if microseconds==0.
            # https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
            # https://docs.python.org/3/library/datetime.html#datetime.datetime.isoformat
            timestamp = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            doc['timestamp'] = timestamp
            # ES6 deprecates types, ES7 will drop them. We use indices as buckets, and types are
            # some configured / fixed string as they are still mandated by API, but that can be
            # easily dropped in future.
            if self.es_type:
                req = {"index": {"_index": bucket, "_id": str(uuid.uuid4()), "_type": self.es_type}}
            else:
                req = {"index": {"_index": bucket, "_id": str(uuid.uuid4())}}
            buffer.append(json.dumps(req, indent=None))
            buffer.append('\n')
            buffer.append(json.dumps(doc, indent=None))
            buffer.append('\n')
        body = ''.join(buffer).encode('utf-8')
        headers = {
            # ES complains when receiving the content type with charset specified, even though
            # it does specify charset in its responses...
            # 'Content-Type': 'application/x-ndjson; charset=UTF-8'
            'Content-Type': 'application/x-ndjson'
        }
        self.request('POST', '/_bulk', body=body, headers=headers)
        resp = self.getresponse()
        if resp.status != 200:
            raise ConnectionError('Elasticsearch error code {}'.format(resp.status))
        return json.loads(resp.read().decode('utf-8'))


class ElasticsearchClient(module.MetricsPushProcess, module.TCPConnector):
    def __init__(self, *args):
        super().__init__(*args, default_port=9200)

    def init_config(self):
        super().init_config()
        self.elasticsearch_name = self.cfg['elasticsearch_name']

    def push_buffer(self):
        if not self.buffer:
            return
        socket = self.get_tcp_connection()
        if not socket:
            return
        elasticsearch_connection = ElasticsearchConnection(
            self.get_tcp_connection, self.elasticsearch_name, self.elasticsearch_name
        )
        for i in range(0, len(self.buffer), self.chunk_size):
            chunk = self.buffer[i:i + self.chunk_size]
            elasticsearch_connection.bulk_upload(chunk)

    def process_values(self, recv_timestamp, bucket, values, timestamp, metadata):
        self.merge_dict(metadata)
        self.merge_dict(values, metadata)
        self.buffer.append((bucket, timestamp or recv_timestamp, values))
