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


import time
import bucky3.module as module


class StatsDServer(module.MetricsSrcProcess, module.UDPConnector):
    def __init__(self, *args):
        super().__init__(*args)
        self.socket = None
        self.timers = {}
        self.histograms = {}
        self.gauges = {}
        self.counters = {}
        self.sets = {}
        self.last_timestamp = 0

    def flush(self, system_timestamp):
        self.enqueue_timers(system_timestamp)
        self.enqueue_histograms(system_timestamp)
        self.enqueue_counters(system_timestamp)
        self.enqueue_gauges(system_timestamp)
        self.enqueue_sets(system_timestamp)
        self.last_timestamp = system_timestamp
        return super().flush(system_timestamp)

    def init_config(self):
        super().init_config()
        percentile_thresholds = self.cfg.get('percentile_thresholds', ())
        self.percentile_thresholds = sorted(set(round(float(t), 2) for t in percentile_thresholds if t > 0 and t <= 100))
        self.histogram_selector = self.cfg.get('histogram_selector')
        self.timestamp_window = self.cfg.get('timestamp_window', 600)

    def loop(self):
        socket = self.get_udp_socket(bind=True)
        while True:
            try:
                data, addr = socket.recvfrom(65535)
                self.handle_packet(data, addr)
            except InterruptedError:
                pass

    def enqueue_timers(self, system_timestamp):
        interval = system_timestamp - self.last_timestamp
        bucket = self.cfg['timers_bucket']
        timestamp = system_timestamp if self.add_timestamps else None
        for k, (cust_timestamp, v) in self.timers.items():
            v.sort()
            count = len(v)
            thresholds = ((count if t == 100 else (t * count) // 100, t) for t in self.percentile_thresholds)

            try:
                next_i, next_t = next(thresholds)
                vlen = vsum = vsum_squares = 0
                for i, x in enumerate(v):
                    vlen += 1
                    vsum += x
                    vsum_squares += x * x
                    while i >= next_i - 1:
                        mean = vsum / vlen
                        stats = dict(count=vlen, count_ps=vlen / interval, lower=v[0], upper=x, mean=mean)
                        if vlen > 1:
                            var = (vsum_squares - 2 * mean * vsum + vlen * mean * mean) / (vlen - 1)
                            stats['stdev'] = var ** 0.5
                        metadata = dict(percentile=str(next_t))
                        metadata.update(k)
                        self.buffer_metric(bucket, stats, cust_timestamp or timestamp, metadata)
                        next_i, next_t = next(thresholds)
            except StopIteration:
                pass
        self.timers = {}

    def enqueue_histograms(self, system_timestamp):
        interval = system_timestamp - self.last_timestamp
        bucket = self.cfg['histograms_bucket']
        timestamp = system_timestamp if self.add_timestamps else None
        for k, (cust_timestamp, selector, buckets) in self.histograms.items():
            for histogram_bucket, (vlen, vsum, vsum_squares, vmin, vmax) in buckets.items():
                mean = vsum / vlen
                stats = dict(count=vlen, count_ps=vlen / interval, lower=vmin, upper=vmax, mean=mean)
                if vlen > 1:
                    var = (vsum_squares - 2 * mean * vsum + vlen * mean * mean) / (vlen - 1)
                    stats['stdev'] = var ** 0.5
                metadata = dict(histogram=str(histogram_bucket))
                metadata.update(k)
                self.buffer_metric(bucket, stats, cust_timestamp or timestamp, metadata)
        self.histograms = {}

    def enqueue_sets(self, system_timestamp):
        bucket = self.cfg['sets_bucket']
        timestamp = system_timestamp if self.add_timestamps else None
        for k, (cust_timestamp, v) in self.sets.items():
            self.buffer_metric(bucket, {"count": len(v)}, cust_timestamp or timestamp, dict(k))
        self.sets = {}

    def enqueue_gauges(self, system_timestamp):
        bucket = self.cfg['gauges_bucket']
        timestamp = system_timestamp if self.add_timestamps else None
        for k, (cust_timestamp, v) in self.gauges.items():
            self.buffer_metric(bucket, {"value": float(v)}, cust_timestamp or timestamp, dict(k))
        self.gauges = {}

    def enqueue_counters(self, system_timestamp):
        interval = system_timestamp - self.last_timestamp
        bucket = self.cfg['counters_bucket']
        timestamp = system_timestamp if self.add_timestamps else None
        for k, (cust_timestamp, v) in self.counters.items():
            stats = {
                'rate': float(v) / interval,
                'count': float(v)
            }
            self.buffer_metric(bucket, stats, cust_timestamp or timestamp, dict(k))
        self.counters = {}

    def handle_packet(self, data, addr=None):
        # Adding a bit of extra sauce so clients can send multiple samples in a single UDP packet.
        try:
            recv_timestamp, data = round(time.time(), 3), data.decode("ascii")
        except UnicodeDecodeError:
            return
        for line in data.splitlines():
            line = line.strip()
            if line:
                self.handle_line(recv_timestamp, line)

    def handle_line(self, recv_timestamp, line):
        # DataDog special packets for service check and events, ignore them
        if line.startswith('sc|') or line.startswith('_e{'):
            return
        try:
            cust_timestamp, line, metadata = self.handle_metadata(recv_timestamp, line)
        except ValueError:
            return
        if not line:
            return
        bits = line.split(":")
        if len(bits) < 2:
            return
        name = bits.pop(0)
        if not name.isidentifier():
            return
        key, metadata = self.handle_key(name, metadata)
        if not key:
            return

        # I'm not sure if statsd is doing this on purpose but the code allows for name:v1|t1:v2|t2 etc.
        # In the interest of compatibility, I'll maintain the behavior.
        for sample in bits:
            if "|" not in sample:
                continue
            fields = sample.split("|")
            valstr = fields[0]
            if not valstr:
                continue
            typestr = fields[1]
            ratestr = fields[2] if len(fields) > 2 else None
            try:
                if typestr == "ms" or typestr == "h":
                    self.handle_timer(cust_timestamp, key, metadata, valstr, ratestr)
                elif typestr == "g":
                    self.handle_gauge(cust_timestamp, key, metadata, valstr, ratestr)
                elif typestr == "s":
                    self.handle_set(cust_timestamp, key, metadata, valstr, ratestr)
                else:
                    self.handle_counter(cust_timestamp, key, metadata, valstr, ratestr)
            except ValueError:
                pass

    def handle_metadata(self, recv_timestamp, line):
        # http://docs.datadoghq.com/guides/dogstatsd/#datagram-format
        bits = line.split("|#", 1)  # We allow '#' in tag values, too
        cust_timestamp, metadata = None, {}
        if len(bits) < 2:
            return cust_timestamp, line, metadata
        for i in bits[1].split(","):
            if not i:
                continue
            # Due to how we parse the metadata, comma is the only illegal character
            # in tag values, everything else will be taken literally.
            # Prometheus and Influx modules handle escaping the special chars as needed.
            # There is no special char handling in carbon module at all, i.e. it is flawed.
            k, _, v = i.partition('=')
            if not k.isidentifier() or not v:
                raise ValueError()
            if k == 'timestamp':
                cust_timestamp = float(v)
                # 2524608000 = secs from epoch to 1 Jan 2050
                if cust_timestamp > 2524608000:
                    cust_timestamp /= 1000
                if abs(recv_timestamp - cust_timestamp) > self.timestamp_window:
                    raise ValueError()
                cust_timestamp = round(cust_timestamp, 3)
            elif k == 'bucket':
                if not v.isidentifier():
                    raise ValueError()
                metadata[k] = v
            else:
                metadata[k] = v
        return cust_timestamp, bits[0], metadata

    def handle_key(self, name, metadata):
        metadata.update(name=name)
        key = tuple((k, metadata[k]) for k in sorted(metadata.keys()))
        return key, metadata

    def handle_timer(self, cust_timestamp, key, metadata, valstr, ratestr):
        val = float(valstr)

        if key in self.timers:
            buf = self.timers[key][1]
            buf.append(val)
            self.timers[key] = cust_timestamp, buf
        else:
            self.timers[key] = cust_timestamp, [val]

        if self.histogram_selector is None:
            return

        histogram = self.histograms.get(key)
        if histogram is None:
            selector = self.histogram_selector(metadata)
            if selector is None:
                return
            buckets = {}
        else:
            selector = histogram[1]
            buckets = histogram[2]
        bucket_name = selector(val)
        if bucket_name:
            bucket_stats = buckets.get(bucket_name)
            if bucket_stats:
                vlen, vsum, vsum_squares, vmin, vmax = bucket_stats
            else:
                vlen = vsum = vsum_squares = 0
                vmin = vmax = val
            buckets[bucket_name] = (
                vlen + 1, vsum + val, vsum_squares + val * val, min(val, vmin), max(val, vmax)
            )
            self.histograms[key] = cust_timestamp, selector, buckets
            return

    def handle_gauge(self, cust_timestamp, key, metadata, valstr, ratestr):
        val = float(valstr)
        delta = valstr[0] in "+-"
        if delta and key in self.gauges:
            self.gauges[key] = cust_timestamp, self.gauges[key][1] + val
        else:
            self.gauges[key] = cust_timestamp, val

    def handle_set(self, cust_timestamp, key, metadata, valstr, ratestr):
        if key in self.sets:
            buf = self.sets[key][1]
            buf.add(valstr)
            self.sets[key] = cust_timestamp, buf
        else:
            self.sets[key] = cust_timestamp, {valstr}

    def handle_counter(self, cust_timestamp, key, metadata, valstr, ratestr):
        if ratestr and ratestr[0] == "@":
            rate = float(ratestr[1:])
            if rate > 0 and rate <= 1:
                val = float(valstr) / rate
            else:
                return
        else:
            val = float(valstr)
        if key in self.counters:
            val += self.counters[key][1]
        self.counters[key] = cust_timestamp, val
