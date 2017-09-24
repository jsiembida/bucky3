

import os
import io
import sys
import time
import string
import random
import pstats
import unittest
import cProfile
import itertools
import statistics
from unittest.mock import patch, MagicMock
import bucky3.statsd as statsd


class RoughFloat(float):
    def __eq__(self, other):
        if not isinstance(other, float):
            return super().__eq__(other)
        return round(self, 2) == round(other, 2)


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
            if callable(timestamps):
                system_time.side_effect, monotonic_time.side_effect = itertools.tee((t for t in timestamps()), 2)
            else:
                system_time.side_effect, monotonic_time.side_effect = itertools.tee(timestamps, 2)
            cfg = dict(
                flush_interval=1,
                timers_timeout=100, timers_bucket="stats_timers",
                histograms_timeout=100, histograms_bucket="stats_histograms",
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


def single_histogram_1_bucket(key):
    return (
        ('under_300', lambda x: x < 300),
    )


def single_histogram_3_buckets(key):
    return (
        ('under_100', lambda x: x < 100),
        ('under_300', lambda x: x < 300),
        ('over_300', lambda x: True),
    )


def single_histogram_10_buckets(key):
    return (
        ('under_100', lambda x: x < 100),
        ('under_200', lambda x: x < 200),
        ('under_300', lambda x: x < 300),
        ('under_400', lambda x: x < 400),
        ('under_500', lambda x: x < 500),
        ('under_600', lambda x: x < 600),
        ('under_700', lambda x: x < 700),
        ('under_800', lambda x: x < 800),
        ('under_900', lambda x: x < 900),
        ('over_900', lambda x: True),
    )


def multiple_histogram_selector(key):
    if key['name'] == 'gorm':
        return (
            ('gorm_under_100', lambda x: x < 100),
            ('gorm_over_100', lambda x: True),
        )
    if key['name'] == 'gurm':
        return (
            ('gurm_under_300', lambda x: x < 300),
            ('gurm_under_1000', lambda x: x < 1000),
            ('gurm_over_1000', lambda x: True),
        )


class TestStatsDServer(unittest.TestCase):
    def malformed_entries(self, statsd_module, entry_type, check_numeric=True, check_rate=False):
        mock_pipe = statsd_module.dst_pipes[0]

        def test(s):
            statsd_module.handle_packet(s.encode("utf-8"))
            statsd_module.tick()
            assert not mock_pipe.called
            assert not mock_pipe.send.called
            mock_pipe.reset_mock()

        test(":1|" + entry_type)
        test("g.o.r.m:1|" + entry_type)
        test("gérm:1|" + entry_type)
        test("gorm:|" + entry_type)
        if check_numeric:
            test("gorm:abc|" + entry_type)
        if check_rate:
            test("gorm:1|" + entry_type + "|@")
            test("gorm:1|" + entry_type + "|@0")
            test("gorm:1|" + entry_type + "|@1.1")
            test("gorm:1|" + entry_type + "|@-0.3")

    def malformed_metadata(self, statsd_module, entry):
        mock_pipe = statsd_module.dst_pipes[0]
        legal_name_chars = string.ascii_letters
        illegal_name_chars = '''-+@?#./%<>*;&[], '"'''
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
            assert not mock_pipe.send.called
            mock_pipe.reset_mock()
            i += 1

        for c in illegal_value_chars:
            name, value = get_token(legal_name_chars), get_token(legal_value_chars, c)
            statsd_module.handle_line(i, entry + '|#' + name + '=' + value)
            statsd_module.tick()
            assert not mock_pipe.called
            assert not mock_pipe.send.called
            mock_pipe.reset_mock()
            i += 1

    def timestamped_metadata(self, statsd_module, entry):
        mock_pipe = statsd_module.dst_pipes[0]

        def test(condition, s):
            statsd_module.handle_packet((entry + "|#timestamp=" + s).encode("ascii"))
            statsd_module.tick()
            assert not mock_pipe.called
            assert mock_pipe.send.called == condition
            mock_pipe.reset_mock()

        test(False, "")
        test(False, "not-a-timestamp")
        test(False, "-1000")  # Beyond 10min window
        test(False, "1000")   # Beyond 10min window
        test(True, "-123")    # Within 10min window
        test(True, "123.4")   # Within 10min window

    def bucketed_metadata(self, statsd_module, entry, expected_metadata_size=2):
        mock_pipe = statsd_module.dst_pipes[0]

        def test(condition, s):
            statsd_module.handle_packet((entry + "|#hello=world,bucket=" + s).encode("ascii"))
            statsd_module.tick()
            assert not mock_pipe.called
            assert mock_pipe.send.called == condition
            if condition:
                args, kwargs = mock_pipe.send.call_args
                assert len(args) == 1
                payload = args[0]
                assert len(payload) == 1
                payload = payload[0]
                assert payload[0] == s
                assert len(payload[3]) == expected_metadata_size
            mock_pipe.reset_mock()

        test(False, "")
        test(False, "not-a-bucket-name")
        test(True, "valid_bucket_name")

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
        self.malformed_entries(statsd_module, 'c', check_rate=True)

    @statsd_setup(timestamps=range(1, 1000))
    def test_malformed_counters_metadata(self, statsd_module):
        self.malformed_metadata(statsd_module, "gorm:1|c")

    @statsd_setup(timestamps=range(1, 1000))
    def test_timestamped_counters_metadata(self, statsd_module):
        self.timestamped_metadata(statsd_module, "gorm:1|c")

    @statsd_setup(timestamps=range(1, 1000))
    def test_bucketed_counters_metadata(self, statsd_module):
        self.bucketed_metadata(statsd_module, "gorm:1|c")

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

    @statsd_setup(timestamps=range(1, 1000))
    def test_timestamped_gauges_metadata(self, statsd_module):
        self.timestamped_metadata(statsd_module, "gorm:1|g")

    @statsd_setup(timestamps=range(1, 1000))
    def test_bucketed_gauges_metadata(self, statsd_module):
        self.bucketed_metadata(statsd_module, "gorm:1|g")

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

    @statsd_setup(timestamps=range(1, 1000))
    def test_timestamped_sets_metadata(self, statsd_module):
        self.timestamped_metadata(statsd_module, "gorm:x|s")

    @statsd_setup(timestamps=range(1, 1000))
    def test_bucketed_sets_metadata(self, statsd_module):
        self.bucketed_metadata(statsd_module, "gorm:x|s")

    @statsd_setup(timers_timeout=0.3,
                  flush_interval=0.1,
                  percentile_thresholds=(90,),
                  timestamps=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7))
    def test_single_timer_sample(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        statsd_module.handle_line(0, "gorm:100|ms")
        expected_value = {
            "mean": 100.0,
            "upper": 100.0,
            "lower": 100.0,
            "count": 1,
            "count_ps": 10.0,
            "sum": 100.0,
            "sum_squares": 10000.0,
        }
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', expected_value, 0.1, dict(name='gorm', percentile='90.0'))
        ])
        statsd_module.handle_line(0.1, "gorm:100|ms")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', expected_value, 0.2, dict(name='gorm', percentile='90.0'))
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
            "mean": 150,
            "lower": 100,
            "upper": 200,
            "count": 2,
            "count_ps": 20,
            "sum": 300,
            "sum_squares": 50000,
            "stdev": 70.71067811865476
        }
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', expected_value, 0.1, dict(name='gorm', percentile='90.0'))
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
            "mean": 1,
            "lower": 1,
            "upper": 1,
            "count": 9,
            "count_ps": 18.0,
            "sum": 9,
            "sum_squares": 9,
            "stdev": 0.0
        }
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', expected_value, 0.5, dict(name='gorm', percentile='90.0'))
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
            "mean": 10 / 3.0,
            "lower": 2,
            "upper": 5,
            "count": 3,
            "count_ps": 6,
            "sum": 10,
            "sum_squares": 38,
            "stdev": 1.5275252316519463
        }
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', expected_value, 0.5, dict(name='gorm', percentile='90.0'))
        ])

    _percentile_thresholds = (10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 97, 98, 99, 99.9, 100)

    @statsd_setup(timers_timeout=100, timestamps=range(1, 100), percentile_thresholds=_percentile_thresholds)
    def test_timer_large_series(self, statsd_module):
        test_name = 'gorm'
        test_vector = self.rand_vec(length=3000)
        for sample in test_vector:
            statsd_module.handle_line(0, test_name + ":" + str(sample) + "|ms")
        statsd_module.tick()
        test_vector.sort()
        expected_values = []
        for threshold_v in self._percentile_thresholds:
            threshold_i = len(test_vector) if threshold_v == 100 else (threshold_v * len(test_vector)) // 100
            threshold_slice = test_vector[:int(threshold_i)]
            expected_value = {
                "mean": RoughFloat(statistics.mean(threshold_slice)),
                "upper": RoughFloat(max(threshold_slice)),
                "lower": RoughFloat(min(threshold_slice)),
                "count": len(threshold_slice),
                "count_ps": len(threshold_slice),
                "sum": RoughFloat(sum(threshold_slice)),
                "sum_squares": RoughFloat(sum(i * i for i in threshold_slice)),
                "stdev": RoughFloat(statistics.stdev(threshold_slice))
            }
            expected_values.append(('stats_timers', expected_value, 1,
                                    dict(name=test_name, percentile=str(float(threshold_v)))))
        statsd_verify(statsd_module.dst_pipes[0], expected_values)

    @statsd_setup(timers_timeout=2, timestamps=range(1, 100), percentile_thresholds=(100,))
    def test_timers_metadata(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        expected_value = {
            "mean": 100.0,
            "upper": 100.0,
            "lower": 100.0,
            "count": 1,
            "count_ps": 1.0,
            "sum": 100.0,
            "sum_squares": 10000.0,
        }
        expected_value2 = expected_value.copy()
        expected_value2.update(count=2, count_ps=2.0, sum=200.0, sum_squares=20000.0, stdev=0.0)
        statsd_module.handle_line(0, "gorm:100|ms")
        statsd_module.handle_line(0, "gorm:100|ms|#a=b")
        statsd_module.handle_line(0, "gorm:100|ms|#a:b,c=5")
        statsd_module.handle_line(0, "gorm:100|ms|#a:z,c=5")
        statsd_module.handle_line(0, "gorm:100|ms|#c:5,a=b")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', expected_value, 1, dict(name='gorm', percentile='100.0')),
            ('stats_timers', expected_value, 1, dict(name='gorm', a='b', percentile='100.0')),
            ('stats_timers', expected_value2, 1, dict(name='gorm', a='b', c='5', percentile='100.0')),
            ('stats_timers', expected_value, 1, dict(name='gorm', a='z', c='5', percentile='100.0')),
        ])
        statsd_module.handle_line(1, "gorm:100|ms|#a:b,c=5")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_timers', dict(count=0, count_ps=0), 2, dict(name='gorm')),
            ('stats_timers', dict(count=0, count_ps=0), 2, dict(name='gorm', a='b')),
            ('stats_timers', expected_value, 2, dict(name='gorm', a='b', c='5', percentile='100.0')),
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

    @statsd_setup(timestamps=range(1, 1000), percentile_thresholds=(100,))
    def test_timestamped_timers_metadata(self, statsd_module):
        self.timestamped_metadata(statsd_module, "gorm:1|ms")

    @statsd_setup(timestamps=range(1, 1000), percentile_thresholds=(100,))
    def test_bucketed_timers_metadata(self, statsd_module):
        self.bucketed_metadata(statsd_module, "gorm:1|ms", expected_metadata_size=3)

    @statsd_setup(timers_timeout=0.3,
                  flush_interval=0.1,
                  timestamps=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7),
                  histogram_selector=lambda key: (('test_histogram', lambda x: True,),))
    def test_histogram_samples1(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        statsd_module.handle_line(0, "gorm:100|h")
        expected_value = {
            "mean": 100,
            "lower": 100,
            "upper": 100,
            "count": 1,
            "count_ps": 10,
            "sum": 100,
            "sum_squares": 100*100
        }
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_histograms', expected_value, 0.1, dict(name='gorm', histogram='test_histogram'))
        ])

    @statsd_setup(timers_timeout=0.3,
                  flush_interval=0.1,
                  timestamps=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7),
                  histogram_selector=lambda key: (('test_histogram', lambda x: True,),))
    def test_histogram_samples2(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        statsd_module.handle_line(0, "gorm:100|h")
        statsd_module.handle_line(0, "gorm:200|h|@0.2")
        statsd_module.handle_line(0, "gorm:300|h")
        expected_value = {
            "mean": 200,
            "lower": 100,
            "upper": 300,
            "count": 3,
            "count_ps": 30,
            "sum": 600,
            "sum_squares": 100*100 + 200*200 + 300*300,
            "stdev": 100.0
        }
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_histograms', expected_value, 0.1, dict(name='gorm', histogram='test_histogram'))
        ])

    @statsd_setup(timers_timeout=0.3,
                  flush_interval=0.1,
                  timestamps=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7),
                  histogram_selector=multiple_histogram_selector)
    def test_histogram_large_series(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        test_samples = dict(gorm={}, gurm={}, foo={})
        for i in range(3000):
            name = random.choice(tuple(test_samples.keys()))
            value = random.randint(0, 1500)
            statsd_module.handle_line(0, name + ":" + str(value) + "|h")
            selectors = multiple_histogram_selector(dict(name=name)) or ()
            for k, selector in selectors:
                if selector(value):
                    test_samples[name].setdefault(k, []).append(value)
                    break
        expected_values = []
        for name, d in test_samples.items():
            for k, v in d.items():
                expected_value = {
                    "mean": RoughFloat(statistics.mean(v)),
                    "lower": min(v),
                    "upper": max(v),
                    "count": len(v),
                    "count_ps": len(v) * 10,
                    "sum": sum(v),
                    "sum_squares": RoughFloat(sum(i * i for i in v)),
                }
                if len(v) > 1:
                    expected_value['stdev'] = RoughFloat(statistics.stdev(v))
                expected_values.append(
                    ('stats_histograms', expected_value, 0.1, dict(name=name, histogram=k))
                )
        statsd_module.tick()
        statsd_verify(mock_pipe, expected_values)

    @statsd_setup(timestamps=range(1, 1000))
    def test_malformed_histograms(self, statsd_module):
        self.malformed_entries(statsd_module, 'h')

    @statsd_setup(timestamps=range(1, 1000))
    def test_malformed_histograms_metadata(self, statsd_module):
        self.malformed_metadata(statsd_module, "gorm:1|h")

    @statsd_setup(timestamps=range(1, 1000), percentile_thresholds=(100,),
                  histogram_selector=lambda key: (('test_histogram', lambda x: True,),))
    def test_timestamped_histograms_metadata(self, statsd_module):
        self.timestamped_metadata(statsd_module, "gorm:1|h")

    @statsd_setup(timestamps=range(1, 1000), percentile_thresholds=(100,),
                  histogram_selector=lambda key: (('test_histogram', lambda x: True,),))
    def test_bucketed_histograms_metadata(self, statsd_module):
        self.bucketed_metadata(statsd_module, "gorm:1|h", expected_metadata_size=3)

    def prepare_performance_test(self):
        flag = os.environ.get('TEST_PERFORMANCE', 'no').lower()
        test_requested = flag in ('yes', 'true', '1')
        if not test_requested:
            self.skipTest("Performance test not requested")
            return None
        flag = os.environ.get('PROFILE_PERFORMANCE', 'no').lower()
        profiler_requested = flag in ('yes', 'true', '1')
        return cProfile.Profile() if profiler_requested else None

    def close_performance_test(self, profiler):
        if profiler:
            buf = io.StringIO()
            stats = pstats.Stats(profiler, stream=buf).sort_stats('cumulative')
            stats.print_stats(0.1)
            print(buf.getvalue())

    def rand_str(self, min_len=3, max_len=10, chars=string.ascii_lowercase):
        return ''.join(random.choice(chars) for i in range(random.randint(min_len, max_len)))

    def rand_num(self, min_len=1, max_len=3):
        return self.rand_str(min_len, max_len, string.digits)

    def rand_val(self, mean=None):
        if mean is None:
            mean = 10
        return round(min(max(0, random.gauss(mean, mean / 10)), 2 * mean), 3)

    def rand_vec(self, length=None, mean=None):
        if length is None:
            length = random.randint(10, 100)
        return list(self.rand_val(mean) for i in range(length))

    def metadata_test_set(self, metric_type, set_size, tags_per_sample):
        buf = set()
        while len(buf) < set_size:
            if tags_per_sample > 0:
                tags_str = ','.join(self.rand_str() + '=' + self.rand_str() for i in range(tags_per_sample))
            else:
                tags_str = ''
            l = self.rand_str() + ':' + self.rand_num() + '|' + metric_type
            if random.random() > 0.5:
                l = l + '|@{:.1f}'.format(random.random())
            if tags_str:
                l = l + '|#' + tags_str
            buf.add(l)

        return buf

    def metadata_performance(self, statsd_module, prefix, metric_type, N, M, set_size, tags_per_sample, profiler=None):
        mock_pipe = statsd_module.dst_pipes[0]
        test_sample_set = self.metadata_test_set(metric_type, set_size, tags_per_sample)
        start_time = time.process_time()
        t = 0
        for i in range(N):
            for j in range(M):
                for sample in test_sample_set:
                    if profiler:
                        profiler.enable()
                    statsd_module.handle_line(t, sample)
                    if profiler:
                        profiler.disable()
            if profiler:
                profiler.enable()
            statsd_module.tick()
            if profiler:
                profiler.disable()
            t += 1
            mock_pipe.reset_mock()
        time_delta = time.process_time() - start_time
        total_samples = N * M * len(test_sample_set)
        us_per_sample = 1000000 * time_delta / total_samples
        print('\n{prefix}: {total_samples:d} samples in {time_delta:.2f}s -> {us_per_sample:.1f}us/sample'.format(
            prefix=prefix, total_samples=total_samples, time_delta=time_delta, us_per_sample=us_per_sample
        ), flush=True, file=sys.stderr)

    @statsd_setup(timestamps=range(1, 10000000))
    def test_counters_performance(self, statsd_module):
        prof = self.prepare_performance_test()
        self.metadata_performance(statsd_module, "counters without tags", 'c', 100, 10, 1000, 0, prof)
        self.metadata_performance(statsd_module, "counters with 3 tags", 'c', 100, 10, 1000, 3, prof)
        self.metadata_performance(statsd_module, "counters with 10 tags", 'c', 100, 10, 1000, 10, prof)
        self.close_performance_test(prof)

    @statsd_setup(timestamps=range(1, 10000000))
    def test_gauges_performance(self, statsd_module):
        prof = self.prepare_performance_test()
        self.metadata_performance(statsd_module, "gauges without tags", 'g', 100, 10, 1000, 0, prof)
        self.metadata_performance(statsd_module, "gauges with 3 tags", 'g', 100, 10, 1000, 3, prof)
        self.metadata_performance(statsd_module, "gauges with 10 tags", 'g', 100, 10, 1000, 10, prof)
        self.close_performance_test(prof)

    @statsd_setup(timestamps=range(1, 10000000))
    def test_sets_performance(self, statsd_module):
        prof = self.prepare_performance_test()
        self.metadata_performance(statsd_module, "sets without tags", 's', 100, 10, 1000, 0, prof)
        self.metadata_performance(statsd_module, "sets with 3 tags", 's', 100, 10, 1000, 3, prof)
        self.metadata_performance(statsd_module, "sets with 10 tags", 's', 100, 10, 1000, 10, prof)
        self.close_performance_test(prof)

    @statsd_setup(timestamps=range(1, 10000000), percentile_thresholds=(90, 99))
    def test_timers_performance(self, statsd_module):
        prof = self.prepare_performance_test()
        self.metadata_performance(statsd_module, "timers without tags", 'ms', 100, 10, 1000, 0, prof)
        self.metadata_performance(statsd_module, "timers with 3 tags", 'ms', 100, 10, 1000, 3, prof)
        self.metadata_performance(statsd_module, "timers with 10 tags", 'ms', 100, 10, 1000, 10, prof)
        self.close_performance_test(prof)

    @statsd_setup(timestamps=range(1, 10000000), percentile_thresholds=(90, 99),
                  histogram_selector=single_histogram_1_bucket)
    def test_histograms_performance1(self, statsd_module):
        prof = self.prepare_performance_test()
        self.metadata_performance(statsd_module, "histogram with 1 bucket, no tags", 'h', 100, 10, 1000, 0, prof)
        self.metadata_performance(statsd_module, "histogram with 1 bucket, 10 tags", 'h', 100, 10, 1000, 10, prof)
        self.close_performance_test(prof)

    @statsd_setup(timestamps=range(1, 10000000), percentile_thresholds=(90, 99),
                  histogram_selector=single_histogram_3_buckets)
    def test_histograms_performance3(self, statsd_module):
        prof = self.prepare_performance_test()
        self.metadata_performance(statsd_module, "histogram with 3 buckets, no tags", 'h', 100, 10, 1000, 0, prof)
        self.metadata_performance(statsd_module, "histogram with 3 buckets, 10 tags", 'h', 100, 10, 1000, 10, prof)
        self.close_performance_test(prof)

    @statsd_setup(timestamps=range(1, 10000000), percentile_thresholds=(90, 99),
                  histogram_selector=single_histogram_10_buckets)
    def test_histograms_performance10(self, statsd_module):
        prof = self.prepare_performance_test()
        self.metadata_performance(statsd_module, "histogram with 10 buckets, no tags", 'h', 100, 10, 1000, 0, prof)
        self.metadata_performance(statsd_module, "histogram with 10 buckets, 10 tags", 'h', 100, 10, 1000, 10, prof)
        self.close_performance_test(prof)

    def percentile_test_set(self, length, N=1):
        buf = []
        for i in range(N):
            name = ('name', self.rand_str(min_len=10, max_len=10))
            vector = self.rand_vec(length=length)
            buf.append((tuple((name,),), vector))
        return buf

    def percentiles_performance(self, statsd_module, prefix, vector_len, N, M, profiler=None):
        total_time, test_set = 0, self.percentile_test_set(vector_len, N)
        for i in range(M):
            statsd_module.enqueue = lambda bucket, stats, timestamp, metadata: None
            statsd_module.timers.clear()
            statsd_module.timers.update((k, (8, 8, v)) for k, v in test_set)
            statsd_module.last_timestamp = 0
            statsd_module.current_timestamp = 10
            start_time = time.process_time()
            if profiler:
                profiler.enable()
            statsd_module.enqueue_timers(10)
            if profiler:
                profiler.disable()
            time_delta = time.process_time() - start_time
            total_time += time_delta
        total_samples = N * M * vector_len
        us_per_sample = 1000000 * total_time / total_samples
        print('\n{prefix}: {total_samples:d} samples in {time_delta:.2f}s -> {us_per_sample:.1f}us/sample'.format(
            prefix=prefix, total_samples=total_samples, time_delta=time_delta, us_per_sample=us_per_sample
        ), flush=True, file=sys.stderr)

    @statsd_setup(timestamps=range(1, 10000000), percentile_thresholds=(90,))
    def test_1percentile1_performance(self, statsd_module):
        prof = self.prepare_performance_test()
        self.percentiles_performance(statsd_module, "1 percentile, 10000 vectors of 10 samples", 10, 10000, 10, prof)
        self.percentiles_performance(statsd_module, "1 percentile, 1000 vectors of 100 samples", 100, 1000, 10, prof)
        self.percentiles_performance(statsd_module, "1 percentile, 100 vectors of 1000 samples", 1000, 100, 10, prof)
        self.percentiles_performance(statsd_module, "1 percentile, 10 vectors of 10000 samples", 10000, 10, 10, prof)
        self.close_performance_test(prof)

    @statsd_setup(timestamps=range(1, 10000000), percentile_thresholds=(50, 90, 99))
    def test_3percentiles_performance(self, statsd_module):
        prof = self.prepare_performance_test()
        self.percentiles_performance(statsd_module, "3 percentiles, 10000 vectors of 10 samples", 10, 10000, 10, prof)
        self.percentiles_performance(statsd_module, "3 percentiles, 1000 vectors of 100 samples", 100, 1000, 10, prof)
        self.percentiles_performance(statsd_module, "3 percentiles, 100 vectors of 1000 samples", 1000, 100, 10, prof)
        self.percentiles_performance(statsd_module, "3 percentiles, 10 vectors of 10000 samples", 10000, 10, 10, prof)
        self.close_performance_test(prof)

    @statsd_setup(timestamps=range(1, 10000000), percentile_thresholds=(10, 20, 30, 40, 50, 60, 70, 80, 90, 100))
    def test_10percentiles_performance(self, statsd_module):
        prof = self.prepare_performance_test()
        self.percentiles_performance(statsd_module, "10 percentiles, 10000 vectors of 10 samples", 10, 10000, 10, prof)
        self.percentiles_performance(statsd_module, "10 percentiles, 1000 vectors of 100 samples", 100, 1000, 10, prof)
        self.percentiles_performance(statsd_module, "10 percentiles, 100 vectors of 1000 samples", 1000, 100, 10, prof)
        self.percentiles_performance(statsd_module, "10 percentiles, 10 vectors of 10000 samples", 10000, 10, 10, prof)
        self.close_performance_test(prof)


if __name__ == '__main__':
    unittest.main()
