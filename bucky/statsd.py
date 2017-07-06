# -*- coding: utf-8 -
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# Copyright 2011 Cloudant, Inc.


import os
import re
import math
import time
import bucky.cfg as cfg
import bucky.common as common


class StatsDServer(common.MetricsSrcProcess, common.UDPConnector):
    def __init__(self, *args):
        super().__init__(*args)
        self.timers = {}
        self.gauges = {}
        self.counters = {}
        self.sets = {}
        self.key_res = (
            (re.compile("\s+"), "_"),
            (re.compile("\/"), "-"),
            (re.compile("[^a-zA-Z_\-0-9\.]"), "")
        )

        self.pct_thresholds = cfg.statsd_percentile_thresholds

        self.keys_seen = set()
        self.delete_counters = cfg.statsd_delete_counters
        self.delete_timers = cfg.statsd_delete_timers
        self.delete_sets = cfg.statsd_delete_sets

        self.enable_timer_mean = cfg.statsd_timer_mean
        self.enable_timer_upper = cfg.statsd_timer_upper
        self.enable_timer_lower = cfg.statsd_timer_lower
        self.enable_timer_count = cfg.statsd_timer_count
        self.enable_timer_count_ps = cfg.statsd_timer_count_ps
        self.enable_timer_sum = cfg.statsd_timer_sum
        self.enable_timer_sum_squares = cfg.statsd_timer_sum_squares
        self.enable_timer_median = cfg.statsd_timer_median
        self.enable_timer_std = cfg.statsd_timer_std

    def tick(self):
        timestamp, buf = int(time.time()), []
        if cfg.delete_timers:
            rem_keys = set(self.timers.keys()) - self.keys_seen
            for k in rem_keys:
                del self.timers[k]
        if cfg.delete_counters:
            rem_keys = set(self.counters.keys()) - self.keys_seen
            for k in rem_keys:
                del self.counters[k]
        if cfg.delete_sets:
            rem_keys = set(self.sets.keys()) - self.keys_seen
            for k in rem_keys:
                del self.sets[k]
        self.enqueue_timers(timestamp, buf)
        self.enqueue_counters(timestamp, buf)
        self.enqueue_gauges(timestamp, buf)
        self.enqueue_sets(timestamp, buf)
        self.keys_seen = set()

    def work(self):
        while True:
            data, addr = self.get_udp_socket(bind=True).recvfrom(65535)
            self.handle_packet(data, addr)

    def coalesce_metadata(self, metadata):
        if not metadata:
            return self.metadata_dict
        if self.metadata_dict:
            tmp = self.metadata_dict.copy()
            tmp.update(metadata)
            return tmp
        return dict(metadata)

    def enqueue_timers(self, timestamp, buf):
        for k, v in self.timers.items():
            timer_name, timer_metadata = k
            timer_stats = {}

            # Skip timers that haven't collected any values
            if not v:
                timer_stats['count'] = 0
                timer_stats['count_ps'] = 0.0
            else:
                v.sort()
                count = len(v)
                vmin, vmax = v[0], v[-1]

                cumulative_values = [vmin]
                cumul_sum_squares_values = [vmin * vmin]
                for i, value in enumerate(v):
                    if i == 0:
                        continue
                    cumulative_values.append(value + cumulative_values[i - 1])
                    cumul_sum_squares_values.append(
                        value * value + cumul_sum_squares_values[i - 1])

                for pct_thresh in self.pct_thresholds:
                    thresh_idx = int(math.floor(pct_thresh / 100.0 * count))
                    if thresh_idx == 0:
                        continue
                    vsum = cumulative_values[thresh_idx - 1]

                    t = int(pct_thresh)
                    t_suffix = "_%s" % (t,)
                    mean = vsum / float(thresh_idx)
                    timer_stats["mean" + t_suffix] = mean
                    vthresh = v[thresh_idx - 1]
                    timer_stats["upper" + t_suffix] = vthresh
                    timer_stats["count" + t_suffix] = thresh_idx
                    timer_stats["sum" + t_suffix] = vsum
                    vsum_squares = cumul_sum_squares_values[thresh_idx - 1]
                    timer_stats["sum_squares" + t_suffix] = vsum_squares

                vsum = cumulative_values[count - 1]
                mean = vsum / float(count)

                timer_stats["mean"] = mean
                timer_stats["upper"] = vmax
                timer_stats["lower"] = vmin
                timer_stats["count"] = count
                timer_stats["count_ps"] = float(count) / self.flush_time
                mid = int(count / 2)
                median = (v[mid - 1] + v[mid]) / 2.0 if count % 2 == 0 else v[mid]
                timer_stats["median"] = median
                timer_stats["sum"] = vsum
                vsum_squares = cumul_sum_squares_values[count - 1]
                timer_stats["sum_squares"] = vsum_squares
                sum_of_diffs = sum(((value - mean) ** 2 for value in v))
                stddev = math.sqrt(sum_of_diffs / count)
                timer_stats["std"] = stddev

            if timer_stats:
                buf.append(cfg.timer_prefix + timer_name, timer_stats, timestamp, timer_metadata)

            self.timers[k] = []

    def enqueue_sets(self, timestamp, buf):
        for k, v in self.sets.items():
            set_name, set_metadata = k
            buf.append(cfg.set_prefix + set_name, {"count": len(v)}, timestamp, set_metadata)
            self.sets[k] = set()

    def enqueue_gauges(self, timestamp, buf):
        for k, v in self.gauges.items():
            gauge_name, gauge_metadata = k
            buf.append(cfg.gauge_prefix + gauge_name, v, timestamp, gauge_metadata)

    def enqueue_counters(self, timestamp, buf):
        for k, v in self.counters.items():
            counter_name, counter_metadata = k
            stats = {
                'rate': v / cfg.interval,
                'count': v
            }
            buf.append(cfg.counter_prefix + counter_name, stats, timestamp, counter_metadata)
            self.counters[k] = 0

    def handle_packet(self, data, addr):
        # Adding a bit of extra sauce so clients can
        # send multiple samples in a single UDP packet.
        data = data.decode()
        for line in data.splitlines():
            line = line.strip()
            if line:
                self.handle_line(line)

    def handle_metadata(self, line):
        # http://docs.datadoghq.com/guides/dogstatsd/#datagram-format
        bits = line.split("#")
        if len(bits) < 2:
            return line, None, None
        metadata = {}
        for i in bits[1].split(","):
            kv = i.split("=")
            if len(kv) > 1:
                metadata[kv[0]] = kv[1]
            else:
                kv = i.split(":")
                if len(kv) > 1:
                    metadata[kv[0]] = kv[1]
                else:
                    metadata[kv[0]] = None
        return bits[0], metadata, tuple((k, metadata[k]) for k in sorted(metadata.keys()))

    def handle_line(self, line):
        # DataDog special packets for service check and events, ignore them
        if line.startswith('sc|') or line.startswith('_e{'):
            return
        line, metadata_dict, metadata_tuple = self.handle_metadata(line)
        bits = line.split(":")
        key = self.handle_key(bits.pop(0), metadata)

        if not bits:
            self.bad_line()
            return

        # I'm not sure if statsd is doing this on purpose
        # but the code allows for name:v1|t1:v2|t2 etc etc.
        # In the interest of compatibility, I'll maintain
        # the behavior.
        for sample in bits:
            if "|" not in sample:
                self.bad_line()
                continue
            fields = sample.split("|")
            if fields[1] == "ms":
                self.handle_timer(key, fields)
            elif fields[1] == "g":
                self.handle_gauge(key, fields)
            elif fields[1] == "s":
                self.handle_set(key, fields)
            else:
                self.handle_counter(key, fields)

    def handle_key(self, key, metadata):
        for (rexp, repl) in self.key_res:
            key = rexp.sub(repl, key)
        key = (key, metadata)
        self.keys_seen.add(key)
        return key

    def handle_timer(self, key, fields):
        try:
            val = float(fields[0] or 0)
            with self.lock:
                self.timers.setdefault(key, []).append(val)
        except:
            self.bad_line()

    def handle_gauge(self, key, fields):
        valstr = fields[0] or "0"
        try:
            val = float(valstr)
        except:
            self.bad_line()
            return
        delta = valstr[0] in ["+", "-"]
        with self.lock:
            if delta and key in self.gauges:
                self.gauges[key] = self.gauges[key] + val
            else:
                self.gauges[key] = val

    def handle_set(self, key, fields):
        valstr = fields[0] or "0"
        with self.lock:
            if key not in self.sets:
                self.sets[key] = set()
            self.sets[key].add(valstr)

    def handle_counter(self, key, fields):
        rate = 1.0
        if len(fields) > 2 and fields[2][:1] == "@":
            try:
                rate = float(fields[2][1:].strip())
            except:
                rate = 1.0
        try:
            val = int(float(fields[0] or 0) / rate)
        except:
            self.bad_line()
            return
        with self.lock:
            if key not in self.counters:
                self.counters[key] = 0
            self.counters[key] += val

    def bad_line(self):
        log.error("StatsD: Invalid line: '%s'", self.line.strip())
