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
import bucky.module as module


class StatsDServer(module.MetricsSrcProcess, module.UDPConnector):
    def __init__(self, *args):
        super().__init__(*args)
        self.socket = None
        self.timers = {}
        self.gauges = {}
        self.counters = {}
        self.sets = {}
        self.current_timestamp = self.last_timestamp = module.monotonic_time()
        self.key_res = (
            (re.compile("\s+"), "_"),
            (re.compile("\/"), "-"),
            (re.compile("[^a-zA-Z_\-0-9\.]"), "")
        )

    def flush(self, monotonic_timestamp, system_timestamp):
        self.last_timestamp = self.current_timestamp
        self.current_timestamp = monotonic_timestamp
        self.enqueue_timers(system_timestamp)
        self.enqueue_counters(system_timestamp)
        self.enqueue_gauges(system_timestamp)
        self.enqueue_sets(system_timestamp)
        return super().flush(monotonic_timestamp, system_timestamp)

    def run(self):
        super().run(loop=False)
        while True:
            self.socket = self.socket or self.get_udp_socket(bind=True)
            data, addr = self.socket.recvfrom(65535)
            self.handle_packet(data, addr)

    def enqueue_timers(self, timestamp):
        interval = self.current_timestamp - self.last_timestamp
        timeout = self.cfg['timers_timeout']
        for k, (timer_timestamp, v) in tuple(self.timers.items()):
            if timestamp - timer_timestamp > timeout:
                del self.timers[k]
                continue

            timer_stats = {}

            if not v:
                # Skip timers that haven't collected any values
                timer_stats['count'] = 0.0
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

                for t in self.cfg['percentile_thresholds']:
                    t_index = int(math.floor(t / 100.0 * count))
                    if t_index == 0:
                        continue
                    vsum = cumulative_values[t_index - 1]
                    t_suffix = "_" + str(int(t))
                    mean = vsum / float(t_index)
                    timer_stats["mean" + t_suffix] = float(mean)
                    vthresh = v[t_index - 1]
                    timer_stats["upper" + t_suffix] = float(vthresh)
                    timer_stats["count" + t_suffix] = float(t_index)
                    timer_stats["sum" + t_suffix] = float(vsum)
                    vsum_squares = cumulative_squares[t_index - 1]
                    timer_stats["sum_squares" + t_suffix] = float(vsum_squares)

                vsum = cumulative_values[count - 1]
                mean = vsum / float(count)
                timer_stats["mean"] = float(mean)
                timer_stats["upper"] = float(vmax)
                timer_stats["lower"] = float(vmin)
                timer_stats["count"] = float(count)
                timer_stats["count_ps"] = float(count) / interval
                mid = int(count / 2)
                median = (v[mid - 1] + v[mid]) / 2.0 if count % 2 == 0 else v[mid]
                timer_stats["median"] = float(median)
                timer_stats["sum"] = float(vsum)
                vsum_squares = cumulative_squares[count - 1]
                timer_stats["sum_squares"] = float(vsum_squares)
                sum_of_diffs = sum(((value - mean) ** 2 for value in v))
                stddev = math.sqrt(sum_of_diffs / count)
                timer_stats["std"] = float(stddev)

            if timer_stats:
                self.buffer.append((self.cfg['timers_bucket'], timer_stats, timestamp, dict(k)))

            self.timers[k] = timer_timestamp, []

    def enqueue_sets(self, timestamp):
        timeout = self.cfg['sets_timeout']
        for k, (set_timestamp, v) in tuple(self.sets.items()):
            if timestamp - set_timestamp <= timeout:
                self.buffer.append((self.cfg['sets_bucket'], {"count": float(len(v))}, timestamp, dict(k)))
                self.sets[k] = set_timestamp, set()
            else:
                del self.sets[k]

    def enqueue_gauges(self, timestamp):
        timeout = self.cfg['gauges_timeout']
        for k, (gauge_timestamp, v) in tuple(self.gauges.items()):
            if timestamp - gauge_timestamp <= timeout:
                self.buffer.append((self.cfg['gauges_bucket'], float(v), timestamp, dict(k)))
            else:
                del self.gauges[k]

    def enqueue_counters(self, timestamp):
        interval = self.current_timestamp - self.last_timestamp
        timeout = self.cfg['counters_timeout']
        for k, (counter_timestamp, v) in tuple(self.counters.items()):
            if timestamp - counter_timestamp <= timeout:
                stats = {
                    'rate': float(v) / interval,
                    'count': float(v)
                }
                self.buffer.append((self.cfg['counters_bucket'], stats, timestamp, dict(k)))
                self.counters[k] = counter_timestamp, 0
            else:
                del self.counters[k]

    def handle_packet(self, data, addr):
        # Adding a bit of extra sauce so clients can
        # send multiple samples in a single UDP packet.
        timestamp, data = round(module.system_time(), 3), data.decode()
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
            val = float(fields[0])
            if key in self.timers:
                buf = self.timers[key][1]
                buf.append(val)
                self.timers[key] = timestamp, buf
            else:
                self.timers[key] = timestamp, [val]
        except ValueError:
            pass

    def handle_gauge(self, timestamp, key, fields):
        try:
            valstr = fields[0]
            val = float(valstr)
            delta = valstr[0] in "+-"
            if delta and key in self.gauges:
                self.gauges[key] = timestamp, self.gauges[key][1] + val
            else:
                self.gauges[key] = timestamp, val
        except ValueError:
            pass

    def handle_set(self, timestamp, key, fields):
        val = fields[0]
        if key in self.sets:
            buf = self.sets[key][1]
            buf.add(val)
            self.sets[key] = timestamp, buf
        else:
            self.sets[key] = timestamp, {val}

    def handle_counter(self, timestamp, key, fields):
        try:
            if len(fields) > 2 and fields[2][0] == "@":
                rate = float(fields[2][1:])
                val = float(fields[0]) / rate
            else:
                val = float(fields[0])
            if key in self.counters:
                val += self.counters[key][1]
            self.counters[key] = timestamp, val
        except ValueError:
            pass
