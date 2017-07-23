

import re
import unittest
from unittest.mock import patch
import bucky3.prometheus as prometheus


def prometheus_verify(prometheus_module, expected_values):
    line_pattern = re.compile('^([^\{]+)({(\S+)})*\s+(\S+)\s+(\S+)$')
    label_pattern = re.compile('^([^=]+)=\"(.+)\"$')
    page = prometheus_module.get_page()
    if page:
        lines = page.split('\n')
        assert lines[-1] == ''
        for l in lines[:-1]:
            m = re.match(line_pattern, l)
            assert m
            bucket_name, _, metadata_str, value_str, timestamp_str = m.groups()
            value = float(value_str)
            timestamp = int(timestamp_str) / 1000
            metadata = {}
            if metadata_str:
                for i in metadata_str.split(','):
                    m = re.match(label_pattern, i)
                    label_name, label_value = m.group(1), m.group(2)
                    metadata[label_name] = label_value
            v = (bucket_name, metadata, value, timestamp)
            if v in expected_values:
                expected_values.remove(v)
            else:
                assert False, str(v) + " was not expected"
    if expected_values:
        assert False, "missing " + str(expected_values.pop())


def prometheus_setup(timestamps, **extra_cfg):
    def run(fun, self):
        with patch('bucky3.module.monotonic_time') as monotonic_time, \
                patch('bucky3.module.system_time') as system_time:
            buf = tuple(timestamps)
            system_time.side_effect = tuple(buf)
            monotonic_time.side_effect = tuple(buf)
            cfg = dict(flush_interval=1, values_timeout=10)
            cfg.update(**extra_cfg)
            prometheus_module = prometheus.PrometheusExporter('prometheus_test', cfg, None)
            prometheus_module.init_config()
            expected_output = fun(self, prometheus_module)
            if expected_output is None:
                return
            prometheus_verify(prometheus_module, expected_output)

    if callable(timestamps):
        fun = timestamps
        timestamps = None
        return lambda self: run(fun, self)
    else:
        def wrapper(fun):
            return lambda self: run(fun, self)

        return wrapper


class TestPrometheusExporter(unittest.TestCase):
    @prometheus_setup(values_timeout=2, timestamps=range(1, 100))
    def test_simple_single_values(self, prometheus_module):
        prometheus_module.process_value('val1', 10, 1)
        prometheus_module.process_value('val2', 12.3, 1)
        prometheus_module.process_value('val1', 13, 1)
        prometheus_module.process_value('val3', 14, 2)
        prometheus_verify(prometheus_module, [
            ('val1', {}, 13, 1),
            ('val2', {}, 12.3, 1),
            ('val3', {}, 14, 2),
        ])
        prometheus_module.flush(3, 3)
        prometheus_verify(prometheus_module, [
            ('val1', {}, 13, 1),
            ('val2', {}, 12.3, 1),
            ('val3', {}, 14, 2),
        ])
        prometheus_module.flush(4, 4)
        prometheus_verify(prometheus_module, [
            ('val3', {}, 14, 2),
        ])
        prometheus_module.flush(5, 5)
        prometheus_verify(prometheus_module, [])

    @prometheus_setup(values_timeout=2, timestamps=range(1, 100))
    def test_single_values(self, prometheus_module):
        prometheus_module.process_value('val1', 10, 1, dict(a='b', b=123))
        prometheus_module.process_value('val2', 11.5, 2, dict(foo='bar'))
        prometheus_module.process_value('val1', 12, 1, dict(a='b', b=123))
        prometheus_module.process_value('val2', 13.8, 1, dict(hello='world'))
        prometheus_verify(prometheus_module, [
            ('val1', dict(a='b', b='123'), 12, 1),
            ('val2', dict(foo='bar'), 11.5, 2),
            ('val2', dict(hello='world'), 13.8, 1),
        ])
        prometheus_module.flush(3, 3)
        prometheus_verify(prometheus_module, [
            ('val1', dict(a='b', b='123'), 12, 1),
            ('val2', dict(foo='bar'), 11.5, 2),
            ('val2', dict(hello='world'), 13.8, 1),
        ])
        prometheus_module.flush(4, 4)
        prometheus_verify(prometheus_module, [
            ('val2', dict(foo='bar'), 11.5, 2),
        ])
        prometheus_module.flush(5, 5)
        prometheus_verify(prometheus_module, [])

    @prometheus_setup(values_timeout=2, timestamps=range(1, 100))
    def test_simple_multi_values(self, prometheus_module):
        prometheus_module.process_values('val1', dict(x=1, y=2), 1)
        prometheus_module.process_values('val2', dict(x=4.1, y=5.5, z=1000), 1)
        prometheus_module.process_values('val1', dict(x=8, z=3.567), 2)
        prometheus_module.process_values('val3', dict(a=0), 1)
        prometheus_verify(prometheus_module, [
            ('val1', dict(value='x'), 8, 2),
            ('val1', dict(value='y'), 2, 1),
            ('val1', dict(value='z'), 3.567, 2),
            ('val2', dict(value='x'), 4.1, 1),
            ('val2', dict(value='y'), 5.5, 1),
            ('val2', dict(value='z'), 1000, 1),
            ('val3', dict(value='a'), 0, 1),
        ])
        prometheus_module.flush(3, 3)
        prometheus_verify(prometheus_module, [
            ('val1', dict(value='x'), 8, 2),
            ('val1', dict(value='y'), 2, 1),
            ('val1', dict(value='z'), 3.567, 2),
            ('val2', dict(value='x'), 4.1, 1),
            ('val2', dict(value='y'), 5.5, 1),
            ('val2', dict(value='z'), 1000, 1),
            ('val3', dict(value='a'), 0, 1),
        ])
        prometheus_module.flush(4, 4)
        prometheus_verify(prometheus_module, [
            ('val1', dict(value='x'), 8, 2),
            ('val1', dict(value='z'), 3.567, 2),
        ])
        prometheus_module.flush(5, 5)
        prometheus_verify(prometheus_module, [])

    @prometheus_setup(values_timeout=2, timestamps=range(1, 100))
    def test_multi_values(self, prometheus_module):
        prometheus_module.process_values('val1', dict(x=1.1, y=2), 1, dict(a='b', b=123))
        prometheus_module.process_values('val2', dict(x=4, y=5.6, z=1000), 1, dict(foo='bar'))
        prometheus_module.process_values('val1', dict(x=8.43, z=3.33), 2, dict(a='b', b=123))
        prometheus_module.process_values('val3', dict(a=4, b=5), 1, dict(value=123))
        prometheus_verify(prometheus_module, [
            ('val1', dict(value='x', a='b', b='123'), 8.43, 2),
            ('val1', dict(value='y', a='b', b='123'), 2, 1),
            ('val1', dict(value='z', a='b', b='123'), 3.33, 2),
            ('val2', dict(value='x', foo='bar'), 4, 1),
            ('val2', dict(value='y', foo='bar'), 5.6, 1),
            ('val2', dict(value='z', foo='bar'), 1000, 1),
            ('val3', dict(value='a'), 4, 1),
            ('val3', dict(value='b'), 5, 1),
        ])
        prometheus_module.flush(3, 3)
        prometheus_verify(prometheus_module, [
            ('val1', dict(value='x', a='b', b='123'), 8.43, 2),
            ('val1', dict(value='y', a='b', b='123'), 2, 1),
            ('val1', dict(value='z', a='b', b='123'), 3.33, 2),
            ('val2', dict(value='x', foo='bar'), 4, 1),
            ('val2', dict(value='y', foo='bar'), 5.6, 1),
            ('val2', dict(value='z', foo='bar'), 1000, 1),
            ('val3', dict(value='a'), 4, 1),
            ('val3', dict(value='b'), 5, 1),
        ])
        prometheus_module.flush(4, 4)
        prometheus_verify(prometheus_module, [
            ('val1', dict(value='x', a='b', b='123'), 8.43, 2),
            ('val1', dict(value='z', a='b', b='123'), 3.33, 2),
        ])
        prometheus_module.flush(5, 5)
        prometheus_verify(prometheus_module, [])


if __name__ == '__main__':
    unittest.main()
