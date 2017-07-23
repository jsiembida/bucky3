

import os
import sys
import time
import string
import random
import unittest
from unittest.mock import patch, MagicMock
import bucky3.statsd as statsd


def statsd_verify(output_pipe, expected_values):
    found_values = sum((i[0][0] for i in output_pipe.send.call_args_list), [])
    for v in found_values:
        if v in expected_values:
            expected_values.remove(v)
        else:
            assert False, str(v) + " was not expected"
    if expected_values:
        assert False, "missing " + str(expected_values.pop())
    output_pipe.reset_mock()


def statsd_setup(timestamps, **extra_cfg):
    def run(fun, self):
        with patch('bucky3.module.monotonic_time') as monotonic_time, \
                patch('bucky3.module.system_time') as system_time:
            buf = tuple(timestamps)
            system_time.side_effect = tuple(buf)
            monotonic_time.side_effect = tuple(buf)
            cfg = dict(
                flush_interval=1,
                timers_timeout=100, timers_bucket="stats_timers",
                sets_timeout=100, sets_bucket="stats_sets",
                gauges_timeout=100, gauges_bucket="stats_gauges",
                counters_timeout=100, counters_bucket="stats_counters",
            )
            cfg.update(**extra_cfg)
            output_pipe = MagicMock()
            statsd_module = statsd.StatsDServer('statsd_test', cfg, [output_pipe])
            statsd_module.init_config()
            expected_output = fun(self, statsd_module)
            if expected_output is None:
                return
            statsd_module.tick()
            statsd_verify(output_pipe, expected_output)

    if callable(timestamps):
        fun = timestamps
        timestamps = None
        return lambda self: run(fun, self)
    else:
        def wrapper(fun):
            return lambda self: run(fun, self)

        return wrapper


class TestStatsDServer(unittest.TestCase):
    def malformed_entries(self, statsd_module, entry_type, check_numeric=True):
        mock_pipe = statsd_module.dst_pipes[0]

        def test(s):
            statsd_module.handle_packet((s + entry_type).encode("utf-8"))
            statsd_module.tick()
            assert not mock_pipe.called
            mock_pipe.reset_mock()

        test(":1|")
        test("g.o.r.m:1|")
        test("gérm:1|")
        test("gorm:|")
        if check_numeric:
            test("gorm:abc|")

    def malformed_metadata(self, statsd_module, entry):
        mock_pipe = statsd_module.dst_pipes[0]
        legal_name_chars = string.ascii_letters
        illegal_name_chars = '''-+@?#./%<>*:;&[], '"'''
        legal_value_chars = string.ascii_letters + string.digits + '-+@?#._/%<>*:=;&[]'
        illegal_value_chars = ''', '"'''

        def get_token(legal_chars, illegal_char=None):
            n = random.randint(1, 5) * random.choice(legal_chars)
            if illegal_char:
                n = n + illegal_char + random.randint(1, 5) * random.choice(legal_chars)
            return n

        i = 0

        for c in illegal_name_chars:
            name, value = get_token(legal_name_chars, c), get_token(legal_value_chars)
            statsd_module.handle_line(i, entry + '|#' + name + '=' + value)
            statsd_module.tick()
            assert not mock_pipe.called
            mock_pipe.reset_mock()
            i += 1

        for c in illegal_value_chars:
            name, value = get_token(legal_name_chars), get_token(legal_value_chars, c)
            statsd_module.handle_line(i, entry + '|#' + name + '=' + value)
            statsd_module.tick()
            assert not mock_pipe.called
            mock_pipe.reset_mock()
            i += 1

    @statsd_setup(counters_timeout=3, timestamps=(2, 4, 6, 8, 10, 12, 14))
    def test_counters(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        statsd_module.handle_line(0, "gorm:1.5|c")
        statsd_module.handle_line(0, "gurm:1|c|@0.1")
        statsd_module.handle_line(0, "gorm:3|c")
        statsd_module.handle_line(0, "gorm:0.5|c")
        statsd_module.handle_line(0, "form:10|c|@0.2")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_counters', dict(rate=2.5, count=5), 2, dict(name='gorm')),
            ('stats_counters', dict(rate=5.0, count=10), 2, dict(name='gurm')),
            ('stats_counters', dict(rate=25.0, count=50), 2, dict(name='form'))
        ])
        statsd_module.handle_line(2, "gorm:1|c")
        statsd_module.handle_line(2, "gurm:1.3|c|@0.2")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_counters', dict(rate=0.5, count=1), 4, dict(name='gorm')),
            ('stats_counters', dict(rate=3.25, count=6.5), 4, dict(name='gurm'))
        ])
        statsd_module.handle_line(4, "gurm:3|c|@0.2")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_counters', dict(rate=7.5, count=15), 6, dict(name='gurm'))
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [])

    @statsd_setup(counters_timeout=2, timestamps=range(1, 100))
    def test_counters_metadata(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        statsd_module.handle_line(0, "gorm:1.5|c")
        statsd_module.handle_line(0, "gorm:2.0|c|#a=b")
        statsd_module.handle_line(0, "gorm:2.5|c|#a:b,c=5")
        statsd_module.handle_line(0, "gorm:3.0|c|#a=z,c=5")
        statsd_module.handle_line(0, "gorm:3.5|c|#c:5,a=b")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_counters', dict(rate=1.5, count=1.5), 1, dict(name='gorm')),
            ('stats_counters', dict(rate=2.0, count=2.0), 1, dict(name='gorm', a='b')),
            ('stats_counters', dict(rate=6.0, count=6.0), 1, dict(name='gorm', a='b', c='5')),
            ('stats_counters', dict(rate=3.0, count=3.0), 1, dict(name='gorm', a='z', c='5')),
        ])
        statsd_module.handle_line(1, "gorm:4.0|c|#c:5,a=z")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_counters', dict(rate=0.0, count=0.0), 2, dict(name='gorm')),
            ('stats_counters', dict(rate=0.0, count=0.0), 2, dict(name='gorm', a='b')),
            ('stats_counters', dict(rate=0.0, count=0.0), 2, dict(name='gorm', a='b', c='5')),
            ('stats_counters', dict(rate=4.0, count=4.0), 2, dict(name='gorm', a='z', c='5')),
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_counters', dict(rate=0.0, count=0.0), 3, dict(name='gorm', a='z', c='5')),
        ])

    @statsd_setup(timestamps=range(1, 1000))
    def test_malformed_counters(self, statsd_module):
        self.malformed_entries(statsd_module, 'c')

    @statsd_setup(timestamps=range(1, 1000))
    def test_malformed_counters_metadata(self, statsd_module):
        self.malformed_metadata(statsd_module, "gorm:1|c")

    @statsd_setup(gauges_timeout=3, timestamps=(1, 2, 3, 4, 5, 6, 7, 8))
    def test_gauges(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        statsd_module.handle_line(0, "gorm:6.7|g")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_gauges', 6.7, 1, dict(name='gorm'))
        ])
        statsd_module.handle_line(1, "gorm:3|g|@0.5")
        statsd_module.handle_line(1, "gorm:8.1|g")
        statsd_module.handle_line(1, "gurm:123|g|@0.2")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_gauges', 8.1, 2, dict(name='gorm')),
            ('stats_gauges', 123, 2, dict(name='gurm'))
        ])
        statsd_module.handle_line(2, "gurm:12|g|@0.5")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_gauges', 8.1, 3, dict(name='gorm')),
            ('stats_gauges', 12, 3, dict(name='gurm')),
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_gauges', 8.1, 4, dict(name='gorm')),
            ('stats_gauges', 12, 4, dict(name='gurm'))
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_gauges', 12, 5, dict(name='gurm'))
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [])

    @statsd_setup(gauges_timeout=2, timestamps=range(1, 100))
    def test_gauges_metadata(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        statsd_module.handle_line(0, "gorm:1.5|g")
        statsd_module.handle_line(0, "gorm:2.0|g|#a:b")
        statsd_module.handle_line(0, "gorm:2.5|g|#a=b,c:5")
        statsd_module.handle_line(0, "gorm:3.0|g|#a=z,c=5")
        statsd_module.handle_line(0, "gorm:3.5|g|#c=5,a:b")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_gauges', 1.5, 1, dict(name='gorm')),
            ('stats_gauges', 2.0, 1, dict(name='gorm', a='b')),
            ('stats_gauges', 3.5, 1, dict(name='gorm', a='b', c='5')),
            ('stats_gauges', 3.0, 1, dict(name='gorm', a='z', c='5')),
        ])
        statsd_module.handle_line(1, "gorm:4.0|g|#c=5,a:z")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_gauges', 1.5, 2, dict(name='gorm')),
            ('stats_gauges', 2.0, 2, dict(name='gorm', a='b')),
            ('stats_gauges', 3.5, 2, dict(name='gorm', a='b', c='5')),
            ('stats_gauges', 4.0, 2, dict(name='gorm', a='z', c='5')),
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_gauges', 4.0, 3, dict(name='gorm', a='z', c='5')),
        ])

    @statsd_setup(timestamps=range(1, 1000))
    def test_malformed_gauges(self, statsd_module):
        self.malformed_entries(statsd_module, 'g')

    @statsd_setup(timestamps=range(1, 1000))
    def test_malformed_gauges_metadata(self, statsd_module):
        self.malformed_metadata(statsd_module, "gorm:1|g")

    @statsd_setup(sets_timeout=3, timestamps=(1, 2, 3, 4, 5, 6, 7, 8))
    def test_sets(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        statsd_module.handle_line(0, "gorm:abc|s|@0.2")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_sets', dict(count=1.0), 1, dict(name='gorm'))
        ])
        statsd_module.handle_line(1, "gurm:x|s")
        statsd_module.handle_line(1, "gurm:y|s|@0.2")
        statsd_module.handle_line(1, "gurm:z|s|@0.2")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_sets', dict(count=0.0), 2, dict(name='gorm')),
            ('stats_sets', dict(count=3.0), 2, dict(name='gurm'))
        ])
        statsd_module.handle_line(2, "gurm:y|s|@0.2")
        statsd_module.handle_line(2, "gurm:y|s")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_sets', dict(count=0.0), 3, dict(name='gorm')),
            ('stats_sets', dict(count=1.0), 3, dict(name='gurm'))
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_sets', dict(count=0.0), 4, dict(name='gurm'))
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_sets', dict(count=0.0), 5, dict(name='gurm'))
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [])

    @statsd_setup(sets_timeout=2, timestamps=range(1, 100))
    def test_sets_metadata(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        statsd_module.handle_line(0, "gorm:p|s")
        statsd_module.handle_line(0, "gorm:q|s|#a=b")
        statsd_module.handle_line(0, "gorm:r|s|#a:b,c=5")
        statsd_module.handle_line(0, "gorm:s|s|#a=z,c=5")
        statsd_module.handle_line(0, "gorm:t|s|#c:5,a=b")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_sets', dict(count=1), 1, dict(name='gorm')),
            ('stats_sets', dict(count=1), 1, dict(name='gorm', a='b')),
            ('stats_sets', dict(count=2), 1, dict(name='gorm', a='b', c='5')),
            ('stats_sets', dict(count=1), 1, dict(name='gorm', a='z', c='5')),
        ])
        statsd_module.handle_line(1, "gorm:u|s|#c=5,a:z")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_sets', dict(count=0), 2, dict(name='gorm')),
            ('stats_sets', dict(count=0), 2, dict(name='gorm', a='b')),
            ('stats_sets', dict(count=0), 2, dict(name='gorm', a='b', c='5')),
            ('stats_sets', dict(count=1), 2, dict(name='gorm', a='z', c='5')),
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_sets', dict(count=0), 3, dict(name='gorm', a='z', c='5')),
        ])

    @statsd_setup(timestamps=range(1, 1000))
    def test_malformed_sets(self, statsd_module):
        self.malformed_entries(statsd_module, 's', check_numeric=False)

    @statsd_setup(timestamps=range(1, 1000))
    def test_malformed_sets_metadata(self, statsd_module):
        self.malformed_metadata(statsd_module, "gorm:x|s")

    @statsd_setup(timers_timeout=0.3,
                  flush_interval=0.1,
                  percentile_thresholds=(90,),
                  timestamps=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7))
    def test_single_timer_sample(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        statsd_module.handle_line(0, "gorm:100|ms")
        expected_value = {
            "mean": 100,
            "upper": 100,
            "lower": 100,
            "count": 1,
            "count_ps": 10,
            "median": 100,
            "sum": 100,
            "sum_squares": 10000,
            "std": 0
        }
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', expected_value, 0.1, dict(name='gorm'))
        ])
        statsd_module.handle_line(0.1, "gorm:100|ms")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', expected_value, 0.2, dict(name='gorm'))
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', dict(count=0, count_ps=0), 0.3, dict(name='gorm'))
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [])

    @statsd_setup(timers_timeout=0.3,
                  flush_interval=0.1,
                  percentile_thresholds=(90,),
                  timestamps=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7))
    def test_timer_samples1(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        statsd_module.handle_line(0, "gorm:100|ms")
        statsd_module.handle_line(0, "gorm:200|ms|@0.2")
        statsd_module.handle_line(0, "gorm:300|ms")  # Out of the 90% threshold
        expected_value = {
            "mean_90": 150,
            "upper_90": 200,
            "count_90": 2,
            "sum_90": 300,
            "sum_squares_90": 50000,
            "mean": 200,
            "upper": 300,
            "lower": 100,
            "count": 3,
            "count_ps": 30,
            "median": 200,
            "sum": 600,
            "sum_squares": 140000,
            "std": 81.64965809277261
        }
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', expected_value, 0.1, dict(name='gorm'))
        ])

    @statsd_setup(timers_timeout=1,
                  percentile_thresholds=(90,),
                  timestamps=(0.5, 1.0, 1.5, 2.0, 2.5, 3.0))
    def test_timer_samples2(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        for i in range(9):
            statsd_module.handle_line(0, "gorm:1|ms")
        statsd_module.handle_line(0, "gorm:2|ms")  # Out of the 90% threshold
        expected_value = {
            "mean_90": 1,
            "upper_90": 1,
            "count_90": 9,
            "sum_90": 9,
            "sum_squares_90": 9,
            "mean": 1.1,
            "upper": 2,
            "lower": 1,
            "count": 10,
            "count_ps": 20.0,
            "median": 1,
            "sum": 11,
            "sum_squares": 13,
            "std": 0.3
        }
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', expected_value, 0.5, dict(name='gorm'))
        ])

    @statsd_setup(timers_timeout=1,
                  percentile_thresholds=(90,),
                  timestamps=(0.5, 1.0, 1.5, 2.0, 2.5, 3.0))
    def test_timer_samples3(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        statsd_module.handle_line(0, "gorm:2|ms")
        statsd_module.handle_line(0, "gorm:5|ms")
        statsd_module.handle_line(0, "gorm:7|ms")  # Out of the 90% threshold
        statsd_module.handle_line(0, "gorm:3|ms")
        expected_value = {
            "mean_90": 10 / 3.0,
            "upper_90": 5,
            "count_90": 3,
            "sum_90": 10,
            "sum_squares_90": 38,
            "mean": 17 / 4.0,
            "upper": 7,
            "lower": 2,
            "count": 4,
            "count_ps": 8,
            "median": 4,
            "sum": 17,
            "sum_squares": 87,
            "std": 1.920286436967152
        }
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', expected_value, 0.5, dict(name='gorm'))
        ])

    @statsd_setup(timers_timeout=2, timestamps=range(1, 100))
    def test_timers_metadata(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        expected_value = {
            "mean": 100,
            "upper": 100,
            "lower": 100,
            "count": 1,
            "count_ps": 1,
            "median": 100,
            "sum": 100,
            "sum_squares": 10000,
            "std": 0
        }
        expected_value2 = expected_value.copy()
        expected_value2.update(count=2, count_ps=2, sum=200, sum_squares=20000)
        statsd_module.handle_line(0, "gorm:100|ms")
        statsd_module.handle_line(0, "gorm:100|ms|#a=b")
        statsd_module.handle_line(0, "gorm:100|ms|#a:b,c=5")
        statsd_module.handle_line(0, "gorm:100|ms|#a:z,c=5")
        statsd_module.handle_line(0, "gorm:100|ms|#c:5,a=b")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', expected_value, 1, dict(name='gorm')),
            ('stats_timers', expected_value, 1, dict(name='gorm', a='b')),
            ('stats_timers', expected_value2, 1, dict(name='gorm', a='b', c='5')),
            ('stats_timers', expected_value, 1, dict(name='gorm', a='z', c='5')),
        ])
        statsd_module.handle_line(1, "gorm:100|ms|#a:b,c=5")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', dict(count=0, count_ps=0), 2, dict(name='gorm')),
            ('stats_timers', dict(count=0, count_ps=0), 2, dict(name='gorm', a='b')),
            ('stats_timers', expected_value, 2, dict(name='gorm', a='b', c='5')),
            ('stats_timers', dict(count=0, count_ps=0), 2, dict(name='gorm', a='z', c='5')),
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', dict(count=0, count_ps=0), 3, dict(name='gorm', a='b', c='5')),
        ])

    @statsd_setup(timestamps=range(1, 1000))
    def test_malformed_timers(self, statsd_module):
        self.malformed_entries(statsd_module, 'ms')

    @statsd_setup(timestamps=range(1, 1000))
    def test_malformed_timers_metadata(self, statsd_module):
        self.malformed_metadata(statsd_module, "gorm:1|ms")

    def performance_test_set(self, metric_type, set_size, tags_per_sample):
        def rand_str(min_len=3, max_len=10, chars=string.ascii_lowercase):
            return ''.join(random.choice(chars) for i in range(random.randint(min_len, max_len)))

        def rand_num(min_len=1, max_len=3):
            return rand_str(min_len, max_len, string.digits)

        buf = set()
        while len(buf) < set_size:
            if tags_per_sample > 0:
                tags_str = ','.join(rand_str() + '=' + rand_str() for i in range(tags_per_sample))
            else:
                tags_str = ''
            l = rand_str() + ':' + rand_num() + '|' + metric_type
            if tags_str:
                l = l + '|#' + tags_str
            buf.add(l)

        return buf

    def performance_test(self, statsd_module, prefix, metric_type, N, M, set_size, tags_per_sample):
        flag = os.environ.get('TEST_PERFORMANCE', 'no').lower()
        test_requested = flag == 'yes' or flag == 'true' or flag == '1'
        if not test_requested:
            self.skipTest("Performance test not requested")

        mock_pipe = statsd_module.dst_pipes[0]
        test_sample_set = self.performance_test_set(metric_type, set_size, tags_per_sample)
        start_time = time.process_time()
        t = 0
        for i in range(N):
            for j in range(M):
                for sample in test_sample_set:
                    statsd_module.handle_line(t, sample)
            statsd_module.tick()
            t += 1
            mock_pipe.reset_mock()
        time_delta = time.process_time() - start_time
        micros_per_sample = round(1000000 * time_delta / (N * M * len(test_sample_set)), 3)
        print("\n", prefix, "performance,   us/sample =", micros_per_sample, flush=True, file=sys.stderr)

    @statsd_setup(timestamps=range(1, 10000000))
    def test_counters_performance(self, statsd_module):
        self.performance_test(statsd_module, "counters without tags", 'c', 100, 10, 1000, 0)
        self.performance_test(statsd_module, "counters with 3 tags", 'c', 100, 10, 1000, 3)
        self.performance_test(statsd_module, "counters with 10 tags", 'c', 100, 10, 1000, 10)

    @statsd_setup(timestamps=range(1, 10000000))
    def test_gauges_performance(self, statsd_module):
        self.performance_test(statsd_module, "gauges without tags", 'g', 100, 10, 1000, 0)
        self.performance_test(statsd_module, "gauges with 3 tags", 'g', 100, 10, 1000, 3)
        self.performance_test(statsd_module, "gauges with 10 tags", 'g', 100, 10, 1000, 10)

    @statsd_setup(timestamps=range(1, 10000000))
    def test_sets_performance(self, statsd_module):
        self.performance_test(statsd_module, "sets without tags", 's', 100, 10, 1000, 0)
        self.performance_test(statsd_module, "sets with 3 tags", 's', 100, 10, 1000, 3)
        self.performance_test(statsd_module, "sets with 10 tags", 's', 100, 10, 1000, 10)

    @statsd_setup(timestamps=range(1, 10000000), percentile_thresholds=(90, 99))
    def test_timers_performance(self, statsd_module):
        self.performance_test(statsd_module, "timers without tags", 'ms', 100, 10, 1000, 0)
        self.performance_test(statsd_module, "timers with 3 tags", 'ms', 100, 10, 1000, 3)
        self.performance_test(statsd_module, "timers with 10 tags", 'ms', 100, 10, 1000, 10)


if __name__ == '__main__':
    unittest.main()
