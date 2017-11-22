

import json
import datetime
import string
import random
import unittest
import itertools
from unittest.mock import patch, MagicMock
import bucky3.jsond as jsond


def jsond_verify(output_pipe, expected_values):
    found_values = sum((i[0][0] for i in output_pipe.send.call_args_list), [])
    for v in found_values:
        if v in expected_values:
            expected_values.remove(v)
        else:
            assert False, str(v) + " was not expected"
    if expected_values:
        assert False, "missing " + str(expected_values.pop())
    output_pipe.reset_mock()


def jsond_setup(timestamps, **extra_cfg):
    def run(fun, self):
        with patch('time.monotonic') as monotonic_time, \
                patch('time.time') as system_time:
            if callable(timestamps):
                system_time_mock, monotonic_time_mock = itertools.tee((t for t in timestamps()), 2)
            else:
                system_time_mock, monotonic_time_mock = itertools.tee(timestamps, 2)
            system_time_mock, monotonic_time_mock = iter(system_time_mock), iter(monotonic_time_mock)
            monotonic_time0 = next(monotonic_time_mock)
            # One monotonic tick for self.init_time, we need to inject it
            monotonic_time_mock = itertools.chain(iter([monotonic_time0]), iter([monotonic_time0]), monotonic_time_mock)
            system_time.side_effect = system_time_mock
            monotonic_time.side_effect = monotonic_time_mock
            cfg = dict(
                # log_level=INFO triggers a log line in src module and that calls the mocked system_time
                # which consumes one tick and fails the tests. So up the log_level, really ugly.
                log_level='WARN',
                flush_interval=1,
                add_timestamps=True,
                destination_modules=(),
            )
            cfg.update(**extra_cfg)
            output_pipe = MagicMock()
            tested_module = jsond.JsonDServer('jsond_test', cfg, [output_pipe])
            tested_module.init_config()
            expected_output = fun(self, tested_module)
            if expected_output is None:
                return
            tested_module.tick()
            jsond_verify(output_pipe, expected_output)

    if callable(timestamps):
        fun = timestamps
        timestamps = None
        return lambda self: run(fun, self)
    else:
        def wrapper(fun):
            return lambda self: run(fun, self)

        return wrapper


class TestJsonDServer(unittest.TestCase):
    def randstr(self, size=10):
        return ''.join(random.choice(string.ascii_lowercase) for i in range(size))

    def randnum(self):
        return round(random.random() * 1000000, 6)

    def randlist(self, size=10):
        l = []
        for i in range(size):
            i = random.random()
            if i < 0.1: v = None
            elif i < 0.3: v = bool(random.randint(0, 1))
            elif i < 0.6: v = self.randnum()
            else: v = self.randstr()
            l.append(v)
        return l

    def randdict(self, size=10):
        d = dict()
        for v in self.randlist(size):
            d[self.randstr()] = v
        return d

    @jsond_setup(timestamps=(2, 4, 6, 8, 10, 12, 14, 16, 18, 20))
    def test_single_lines(self, module):
        pipe = module.dst_pipes[0]
        for i in range(5):
            payload_obj = self.randdict()
            payload_str = json.dumps(payload_obj)
            module.handle_line(0, payload_str)
            module.tick()
            jsond_verify(pipe, [
                ('metrics', payload_obj, 0, {}),
            ])
            module.tick()
            jsond_verify(pipe, [])

    @jsond_setup(timestamps=(2, 4, 6, 8, 10, 12, 14, 16, 20))
    def test_multiple_lines(self, module):
        pipe = module.dst_pipes[0]
        for timestamp in 2, 8, 14:
            objs = [self.randdict(), self.randdict(), self.randdict(), self.randdict()]
            module.handle_packet('\n'.join(json.dumps(obj) for obj in objs).encode('utf-8'))
            module.tick()
            jsond_verify(pipe, list(('metrics', obj, timestamp, {}) for obj in objs))
            module.tick()
            jsond_verify(pipe, [])

    @jsond_setup(timestamps=(2, 4, 6, 8, 10, 12, 14, 16, 18, 20))
    def test_malformed_lines(self, module):
        pipe = module.dst_pipes[0]
        payload_obj = self.randlist()
        payload_str = json.dumps(payload_obj)
        module.handle_line(0, payload_str)
        module.tick()
        jsond_verify(pipe, [])
        payload_obj = self.randdict()
        payload_str = json.dumps(payload_obj) + self.randstr()
        module.handle_line(0, payload_str)
        module.tick()
        jsond_verify(pipe, [])

    @jsond_setup(timestamps=(2, 4, 6, 8, 10, 12, 14, 16, 18, 20))
    def test_nested_objects(self, module):
        pipe = module.dst_pipes[0]
        payload_obj = self.randdict()
        payload_obj[self.randstr()] = self.randlist()
        payload_str = json.dumps(payload_obj)
        module.handle_line(0, payload_str)
        module.tick()
        jsond_verify(pipe, [])
        payload_obj = self.randdict()
        payload_obj[self.randstr()] = self.randdict()
        payload_str = json.dumps(payload_obj)
        module.handle_line(0, payload_str)
        module.tick()
        jsond_verify(pipe, [])

    @jsond_setup(timestamps=(2, 4, 6, 8, 10, 12, 14, 16, 18, 20))
    def test_timestamped_lines(self, module):
        pipe = module.dst_pipes[0]
        payload_obj = self.randdict()
        payload_obj['timestamp'] = 1000
        payload_str = json.dumps(payload_obj)
        module.handle_line(0, payload_str)
        del payload_obj['timestamp']
        module.tick()
        jsond_verify(pipe, [
            ('metrics', payload_obj, 0, {}),
        ])

        payload_obj = self.randdict()
        now = datetime.datetime.utcnow()
        payload_obj['timestamp'] = now.timestamp()
        payload_str = json.dumps(payload_obj)
        module.handle_line(0, payload_str)
        del payload_obj['timestamp']
        module.tick()
        jsond_verify(pipe, [
            ('metrics', payload_obj, now.timestamp(), {}),
        ])

        payload_obj = self.randdict()
        now = datetime.datetime.utcnow()
        payload_obj['timestamp'] = now.timestamp() * 1000
        payload_str = json.dumps(payload_obj)
        module.handle_line(0, payload_str)
        del payload_obj['timestamp']
        module.tick()
        jsond_verify(pipe, [
            ('metrics', payload_obj, now.timestamp(), {}),
        ])


if __name__ == '__main__':
    unittest.main()
