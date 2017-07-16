

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
                patch('bucky3.module.system_time') as system_time, \
                patch('bucky3.common.load_config') as load_config:
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
            load_config.side_effect = lambda *args: cfg
            output_pipe = MagicMock()
            statsd_module = statsd.StatsDServer('statsd_test', 'statsd_config', [output_pipe])
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


class TestStatsDModule(unittest.TestCase):
    @statsd_setup(counters_timeout=3, timestamps=(2, 4, 6, 8, 10, 12, 14))
    def test_counters(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        statsd_module.handle_line(0, "gorm:1|c")
        statsd_module.handle_line(0, "gurm:1|c|@0.1")
        statsd_module.handle_line(0, "gorm:3|c")
        statsd_module.handle_line(0, "gorm:1|c")
        statsd_module.handle_line(0, "form:10|c|@0.2")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_counters', dict(rate=2.5, count=5), 2, dict(name='gorm')),
            ('stats_counters', dict(rate=5.0, count=10), 2, dict(name='gurm')),
            ('stats_counters', dict(rate=25.0, count=50), 2, dict(name='form'))
        ])
        statsd_module.handle_line(2, "gorm:1|c")
        statsd_module.handle_line(2, "gurm:1|c|@0.2")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_counters', dict(rate=0.5, count=1), 4, dict(name='gorm')),
            ('stats_counters', dict(rate=2.5, count=5), 4, dict(name='gurm'))
        ])
        statsd_module.handle_line(4, "gurm:3|c|@0.2")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_counters', dict(rate=7.5, count=15), 6, dict(name='gurm'))
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [])

    @statsd_setup(gauges_timeout=3, timestamps=(1, 2, 3, 4, 5, 6, 7, 8))
    def test_gauges(self, statsd_module):
        mock_pipe = statsd_module.dst_pipes[0]
        statsd_module.handle_line(0, "gorm:6|g")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_gauges', 6, 1, dict(name='gorm'))
        ])
        statsd_module.handle_line(1, "gorm:3|g|@0.5")
        statsd_module.handle_line(1, "gorm:8|g")
        statsd_module.handle_line(1, "gurm:123|g|@0.2")
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_gauges', 8, 2, dict(name='gorm')),
            ('stats_gauges', 123, 2, dict(name='gurm'))
        ])
        statsd_module.tick()
        statsd_module.handle_line(2, "gurm:12|g|@0.5")
        statsd_verify(mock_pipe, [
            ('stats_gauges', 8, 3, dict(name='gorm')),
            ('stats_gauges', 123, 3, dict(name='gurm')),
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_gauges', 8, 4, dict(name='gorm')),
            ('stats_gauges', 12, 4, dict(name='gurm'))
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [
            ('stats_gauges', 12, 5, dict(name='gurm'))
        ])
        statsd_module.tick()
        statsd_verify(mock_pipe, [])

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


if __name__ == '__main__':
    unittest.main()
