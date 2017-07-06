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


import re
import math
import time
import bucky.cfg as cfg
import bucky.common as common


class StatsDServer(common.MetricsSrcProcess, common.UDPConnector):
    def __init__(self, *args):
        super().__init__(*args)
        self.socket = None
        self.timers = {}
        self.gauges = {}
        self.counters = {}
        self.sets = {}
        self.key_res = (
            (re.compile("\s+"), "_"),
            (re.compile("\/"), "-"),
            (re.compile("[^a-zA-Z_\-0-9\.]"), "")
        )

    def tick(self):
        timestamp, buf = int(time.time()), []
        self.enqueue_timers(timestamp, buf)
        self.enqueue_counters(timestamp, buf)
        self.enqueue_gauges(timestamp, buf)
        self.enqueue_sets(timestamp, buf)
        self.send_metrics(buf)

    def work(self):
        while True:
            self.socket = self.socket or self.get_udp_socket(bind=True)
            data, addr = self.socket.recvfrom(65535)
            self.handle_packet(data, addr)

    def enqueue_timers(self, timestamp, buf):
        timeout = cfg.timers_timeout
        for k, (timer_timestamp, v) in tuple(self.timers.items()):
            if timestamp - timer_timestamp > timeout:
                del self.timers[k]
                continue

            timer_stats = {}

            if not v:
                # Skip timers that haven't collected any values
                timer_stats['count'] = 0
                timer_stats['count_ps'] = 0.0
            else:
                v.sort()
                count = len(v)
                vmin, vmax = v[0], v[-1]

                cumulative_values = [vmin]
                cumulative_squares = [vmin * vmin]
                for i, value in enumerate(v):
                    if i == 0:
                        continue
                    cumulative_values.append(value + cumulative_values[i - 1])
                    cumulative_squares.append(value * value + cumulative_squares[i - 1])

                for t in cfg.percentile_thresholds:
                    t_index = int(math.floor(t / 100.0 * count))
                    if t_index == 0:
                        continue
                    vsum = cumulative_values[t_index - 1]
                    t_suffix = "_" + str(int(t))
                    mean = vsum / float(t_index)
                    timer_stats["mean" + t_suffix] = mean
                    vthresh = v[t_index - 1]
                    timer_stats["upper" + t_suffix] = vthresh
                    timer_stats["count" + t_suffix] = t_index
                    timer_stats["sum" + t_suffix] = vsum
                    vsum_squares = cumulative_squares[t_index - 1]
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
                vsum_squares = cumulative_squares[count - 1]
                timer_stats["sum_squares"] = vsum_squares
                sum_of_diffs = sum(((value - mean) ** 2 for value in v))
                stddev = math.sqrt(sum_of_diffs / count)
                timer_stats["std"] = stddev

            if timer_stats:
                buf.append((cfg.timers_name, timer_stats, timestamp, dict(k)))

            del self.timers[k]

    def enqueue_sets(self, timestamp, buf):
        timeout = cfg.sets_timeout
        for k, (set_timestamp, v) in tuple(self.sets.items()):
            if timestamp - set_timestamp <= timeout:
                buf.append((cfg.sets_name, {"count": len(v)}, timestamp, dict(k)))
            del self.sets[k]

    def enqueue_gauges(self, timestamp, buf):
        timeout = cfg.gauges_timeout
        for k, (gauge_timestamp, v) in tuple(self.gauges.items()):
            if timestamp - gauge_timestamp <= timeout:
                buf.append((cfg.gauges_name, v, timestamp, dict(k)))
            del self.gauges[k]

    def enqueue_counters(self, timestamp, buf):
        timeout = cfg.counters_timeout
        for k, (counter_timestamp, v) in tuple(self.counters.items()):
            if timestamp - counter_timestamp <= timeout:
                stats = {
                    'rate': v / cfg.interval,
                    'count': v
                }
                buf.append((cfg.counters_name, stats, timestamp, dict(k)))
            del self.counters[k]

    def handle_packet(self, data, addr):
        # Adding a bit of extra sauce so clients can
        # send multiple samples in a single UDP packet.
        timestamp, data = int(time.time()), data.decode()
        for line in data.splitlines():
            line = line.strip()
            if line:
                self.handle_line(timestamp, line)

    def handle_line(self, timestamp, line):
        # DataDog special packets for service check and events, ignore them
        if line.startswith('sc|') or line.startswith('_e{'):
            return
        line, metadata = self.handle_metadata(line)
        bits = line.split(":")
        if len(bits) < 2:
            return
        key = self.handle_key(bits.pop(0), metadata)

        # I'm not sure if statsd is doing this on purpose
        # but the code allows for name:v1|t1:v2|t2 etc etc.
        # In the interest of compatibility, I'll maintain
        # the behavior.
        for sample in bits:
            if "|" not in sample:
                continue
            fields = sample.split("|")
            if fields[1] == "ms":
                self.handle_timer(timestamp, key, fields)
            elif fields[1] == "g":
                self.handle_gauge(timestamp, key, fields)
            elif fields[1] == "s":
                self.handle_set(timestamp, key, fields)
            else:
                self.handle_counter(timestamp, key, fields)

    def handle_metadata(self, line):
        # http://docs.datadoghq.com/guides/dogstatsd/#datagram-format
        bits = line.split("#")
        metadata = {}
        if len(bits) < 2:
            return line, metadata
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
        return bits[0], metadata

    def handle_key(self, key, metadata):
        for (rexp, repl) in self.key_res:
            key = rexp.sub(repl, key)
        metadata.update(name=key)
        key = tuple((k, metadata[k]) for k in sorted(metadata.keys()))
        return key

    def handle_timer(self, timestamp, key, fields):
        try:
            val = float(fields[0] or 0)
            if key in self.timers:
                buf = self.timers[key][1]
                buf.append(val)
                self.timers[key] = timestamp, buf
            else:
                self.timers[key] = timestamp, [val]
        except ValueError:
            pass

    def handle_gauge(self, timestamp, key, fields):
        valstr = fields[0] or "0"
        try:
            val = float(valstr)
        except ValueError:
            return
        delta = valstr[0] in ["+", "-"]
        if delta and key in self.gauges:
            self.gauges[key] = timestamp, self.gauges[key][1] + val
        else:
            self.gauges[key] = timestamp, val

    def handle_set(self, timestamp, key, fields):
        valstr = fields[0]
        if key in self.sets:
            buf = self.sets[key][1]
            buf.add(valstr)
            self.sets[key] = timestamp, buf
        else:
            self.sets[key] = timestamp, set()

    def handle_counter(self, timestamp, key, fields):
        try:
            rate = 1.0
            if len(fields) > 2 and fields[2][:1] == "@":
                rate = float(fields[2][1:].strip())
            val = int(float(fields[0] or 0) / rate)
            if key in self.counters:
                buf = self.counters[key][1]
                buf += val
                self.counters[key] = timestamp, buf
            else:
                self.counters[key] = timestamp, val
        except ValueError:
            pass
