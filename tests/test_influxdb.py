

import unittest
from unittest.mock import patch
import bucky3.influxdb as influxdb


def influxdb_verify(influxdb_module, expected_values):
    for v in influxdb_module.buffer:
        if v in expected_values:
            expected_values.remove(v)
        else:
            assert False, str(v) + " was not expected"
    if expected_values:
        assert False, "missing " + str(expected_values.pop())


def influxdb_setup(timestamps, **extra_cfg):
    def run(fun, self):
        with patch('time.time') as system_time:
            buf = tuple(timestamps)
            system_time.side_effect = tuple(buf)
            cfg = dict(flush_interval=1)
            cfg.update(**extra_cfg)
            influxdb_module = influxdb.InfluxDBClient('influxdb_test', cfg, None)
            influxdb_module.init_config()
            expected_output = fun(self, influxdb_module)
            if expected_output is None:
                return
            influxdb_verify(influxdb_module, expected_output)

    if callable(timestamps):
        fun = timestamps
        timestamps = None
        return lambda self: run(fun, self)
    else:
        def wrapper(fun):
            return lambda self: run(fun, self)

        return wrapper


class TestInfluxDBClient(unittest.TestCase):
    @influxdb_setup(timestamps=range(1, 100))
    def test_simple_single_values(self, influxdb_module):
        influxdb_module.process_value(2, 'val1', 10, 1)
        influxdb_module.process_value(2, 'val2', 11.0, 1)
        influxdb_module.process_value(2, 'val1', 12.7, 2)
        influxdb_module.process_value(2, 'val.3', 13, 1)
        influxdb_module.process_value(2, 'val*3', 14, 1)
        return [
            'val1 value=10i 1000000000',
            'val2 value=11.0 1000000000',
            'val1 value=12.7 2000000000',
            'val.3 value=13i 1000000000',
            'val*3 value=14i 1000000000',
        ]

    @influxdb_setup(timestamps=range(1, 100))
    def test_single_values(self, influxdb_module):
        influxdb_module.process_value(2, 'val1', 10, 1, dict(a='1', b='2'))
        influxdb_module.process_value(2, 'val2', 11, 3, dict(path='/foo/bar', foo='world'))
        influxdb_module.process_value(2, 'val.3', 13.3, 1, dict(path='foo.bar', hello='world'))
        influxdb_module.process_value(2, 'val*3', 14.1, 1, dict(a='foo.bar', hello='world'))
        return [
            'val1,a=1,b=2 value=10i 1000000000',
            'val2,foo=world,path=/foo/bar value=11i 3000000000',
            'val.3,hello=world,path=foo.bar value=13.3 1000000000',
            'val*3,a=foo.bar,hello=world value=14.1 1000000000',
        ]

    @influxdb_setup(timestamps=range(1, 100))
    def test_simple_multi_values(self, influxdb_module):
        influxdb_module.process_values(2, 'val1', dict(x=1.5, y=2), 1)
        influxdb_module.process_values(2, 'val/2', dict(a=1, b=10), 1)
        influxdb_module.process_values(2, 'val1', dict(y=10, z=1.234), 2)
        return [
            'val1 x=1.5,y=2i 1000000000',
            'val/2 a=1i,b=10i 1000000000',
            'val1 y=10i,z=1.234 2000000000',
        ]

    @influxdb_setup(timestamps=range(1, 100))
    def test_multi_values(self, influxdb_module):
        influxdb_module.process_values(2, 'val1', dict(x=1, y=2), 1, dict(path='/foo/bar', foo='world', hello='world'))
        influxdb_module.process_values(2, 'val/2', dict(a=1.2, b=10), 1, dict(a='1', b='2'))
        influxdb_module.process_values(2, 'val1', dict(y=10, z=11.22), 2, dict(path='foo.bar', hello='world'))
        return [
            'val1,foo=world,hello=world,path=/foo/bar x=1i,y=2i 1000000000',
            'val/2,a=1,b=2 a=1.2,b=10i 1000000000',
            'val1,hello=world,path=foo.bar y=10i,z=11.22 2000000000',
        ]


if __name__ == '__main__':
    unittest.main()
