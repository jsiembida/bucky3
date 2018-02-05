

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
            influxdb_module.init_cfg()
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
    def test_simple_multi_values(self, influxdb_module):
        influxdb_module.process_values(2, 'val1', dict(x=1.5, y=2), 1, {})
        influxdb_module.process_values(2, 'val/2', dict(a=1, b=10), 1, {})
        influxdb_module.process_values(2, 'val1', dict(y=10, z=1.234), 2, {})
        return [
            'val1 x=1.5,y=2 1000000000',
            'val/2 a=1,b=10 1000000000',
            'val1 y=10,z=1.234 2000000000',
        ]

    @influxdb_setup(timestamps=range(1, 100))
    def test_multi_values(self, influxdb_module):
        influxdb_module.process_values(2, 'val1', dict(x=1, y=2), 1, dict(path='/foo/bar', foo='world', hello='world'))
        influxdb_module.process_values(2, 'val/2', dict(a=1.2, b=10), 1, dict(a='1', b='2'))
        influxdb_module.process_values(2, 'val1', dict(y=10, z=11.22), 2, dict(path='foo.bar', hello='world'))
        return [
            'val1,foo=world,hello=world,path=/foo/bar x=1,y=2 1000000000',
            'val/2,a=1,b=2 a=1.2,b=10 1000000000',
            'val1,hello=world,path=foo.bar y=10,z=11.22 2000000000',
        ]


if __name__ == '__main__':
    unittest.main()
