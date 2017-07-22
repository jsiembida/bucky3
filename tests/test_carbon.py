

import unittest
from unittest.mock import patch
import bucky3.carbon as carbon


def carbon_verify(carbon_module, expected_values):
    for v in carbon_module.buffer:
        if v in expected_values:
            expected_values.remove(v)
        else:
            assert False, str(v) + " was not expected"
    if expected_values:
        assert False, "missing " + str(expected_values.pop())


def carbon_setup(timestamps, **extra_cfg):
    def run(fun, self):
        with patch('bucky3.module.monotonic_time') as monotonic_time, \
                patch('bucky3.module.system_time') as system_time:
            buf = tuple(timestamps)
            system_time.side_effect = tuple(buf)
            monotonic_time.side_effect = tuple(buf)
            cfg = dict(flush_interval=1, name_mapping=('bucket', 'foo', 'value'))
            cfg.update(**extra_cfg)
            carbon_module = carbon.CarbonClient('carbon_test', cfg, None)
            carbon_module.init_config()
            expected_output = fun(self, carbon_module)
            if expected_output is None:
                return
            carbon_verify(carbon_module, expected_output)

    if callable(timestamps):
        fun = timestamps
        timestamps = None
        return lambda self: run(fun, self)
    else:
        def wrapper(fun):
            return lambda self: run(fun, self)

        return wrapper


class TestCarbonClient(unittest.TestCase):
    @carbon_setup(timestamps=range(1, 100))
    def test_simple_single_values(self, carbon_module):
        carbon_module.process_value('val1', 10, 1)
        carbon_module.process_value('val2', 11, 1)
        carbon_module.process_value('val1', 12, 2)
        carbon_module.process_value('val.3', 13, 1)
        carbon_module.process_value('val*3', 14, 1)
        return [
            'val1 10 1\n',
            'val2 11 1\n',
            'val1 12 2\n',
            'val_3 13 1\n',
            'val_3 14 1\n',
        ]

    @carbon_setup(timestamps=range(1, 100))
    def test_single_values(self, carbon_module):
        carbon_module.process_value('val1', 10, 1, dict(a='1', b='2'))
        carbon_module.process_value('val2', 11, 3, dict(path='/foo/bar', foo='world'))
        carbon_module.process_value('val.3', 13, 1, dict(path='foo.bar', hello='world'))
        carbon_module.process_value('val*3', 14, 1, dict(a='foo.bar', hello='world'))
        return [
            'val1.1.2 10 1\n',
            'val2.world._foo_bar 11 3\n',
            'val_3.world.foo_bar 13 1\n',
            'val_3.foo_bar.world 14 1\n',
        ]

    @carbon_setup(timestamps=range(1, 100))
    def test_simple_multi_values(self, carbon_module):
        carbon_module.process_values('val1', dict(x=1, y=2), 1)
        carbon_module.process_values('val/2', dict(a=1, b=10), 1)
        carbon_module.process_values('val1', dict(y=10, z=11), 2)
        return [
            'val1.x 1 1\n',
            'val1.y 2 1\n',
            'val_2.a 1 1\n',
            'val_2.b 10 1\n',
            'val1.y 10 2\n',
            'val1.z 11 2\n',
        ]

    @carbon_setup(timestamps=range(1, 100))
    def test_multi_values(self, carbon_module):
        carbon_module.process_values('val1', dict(x=1, y=2), 1, dict(path='/foo/bar', foo='world', hello='world'))
        carbon_module.process_values('val/2', dict(a=1, b=10), 1, dict(a='1', b='2'))
        carbon_module.process_values('val1', dict(y=10, z=11), 2, dict(path='foo.bar', hello='world'))
        return [
            'val1.world.x.world._foo_bar 1 1\n',
            'val1.world.y.world._foo_bar 2 1\n',
            'val_2.a.1.2 1 1\n',
            'val_2.b.1.2 10 1\n',
            'val1.y.world.foo_bar 10 2\n',
            'val1.z.world.foo_bar 11 2\n',
        ]


if __name__ == '__main__':
    unittest.main()
