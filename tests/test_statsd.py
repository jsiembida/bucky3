

import unittest
from unittest.mock import patch
from unittest.mock import MagicMock
import bucky3.statsd as statsd


class TestStatsDModule(unittest.TestCase):
    def setup_statsd(self, system_time, monotonic_time, tick_interval=1, values_timeout=100):
        system_time.side_effect = range(10)
        monotonic_time.side_effect = range(10)
        pipe = MagicMock()
        s = statsd.StatsDServer('statsd_test', None, [pipe])
        s.tick_interval = tick_interval
        s.cfg = dict(
            timers_timeout=values_timeout, timers_bucket="stats_timers",
            sets_timeout=values_timeout, sets_bucket="stats_sets",
            gauges_timeout=values_timeout, gauges_bucket="stats_gauges",
            counters_timeout=values_timeout, counters_bucket="stats_counters",
        )
        return s, pipe

    def verify_statsd(self, found_values, expected_values):
        for v in found_values:
            if v in expected_values:
                expected_values.remove(v)
            else:
                assert False, str(v) + " was not expected"
        if expected_values:
            assert False, "missing " + str(expected_values.pop())

    @patch('bucky3.module.monotonic_time')
    @patch('bucky3.module.system_time')
    def test_simple_counter(self, system_time, monotonic_time):
        s, pipe = self.setup_statsd(system_time, monotonic_time)
        s.handle_packet("gorm:3|c".encode(), None)
        s.tick()
        self.verify_statsd(sum((i[0][0] for i in pipe.send.call_args_list), []), [
            ('stats_counters', dict(rate=3.0, count=3), 1, dict(name='gorm'))
        ])

    @patch('bucky3.module.monotonic_time')
    @patch('bucky3.module.system_time')
    def test_multiple_messages(self, system_time, monotonic_time):
        s, pipe = self.setup_statsd(system_time, monotonic_time)
        s.handle_packet("gorm:4|c".encode(), None)
        s.handle_packet("gorm:7|c".encode(), None)
        s.handle_packet("gorm:1|c".encode(), None)
        s.tick()
        self.verify_statsd(sum((i[0][0] for i in pipe.send.call_args_list), []), [
            ('stats_counters', dict(rate=12.0, count=12), 3, dict(name='gorm'))
        ])

    @patch('bucky3.module.monotonic_time')
    @patch('bucky3.module.system_time')
    def test_multiple_counters(self, system_time, monotonic_time):
        s, pipe = self.setup_statsd(system_time, monotonic_time)
        s.handle_packet("gorm:1|c".encode(), None)
        s.handle_packet("gurm:1|c".encode(), None)
        s.tick()
        self.verify_statsd(sum((i[0][0] for i in pipe.send.call_args_list), []), [
            ('stats_counters', dict(rate=1.0, count=1), 2, dict(name='gorm')),
            ('stats_counters', dict(rate=1.0, count=1), 2, dict(name='gurm'))
        ])

    @patch('bucky3.module.monotonic_time')
    @patch('bucky3.module.system_time')
    def test_simple_gauge(self, system_time, monotonic_time):
        s, pipe = self.setup_statsd(system_time, monotonic_time)
        s.handle_packet("gorm:6|g".encode(), None)
        s.handle_packet("gorm:3|g".encode(), None)
        s.handle_packet("gorm:1|g".encode(), None)
        s.tick()
        self.verify_statsd(sum((i[0][0] for i in pipe.send.call_args_list), []), [
            ('stats_gauges', 1, 3, dict(name='gorm'))
        ])


if __name__ == '__main__':
    unittest.main()
