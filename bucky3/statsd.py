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
import bucky3.module as module


class StatsDServer(module.MetricsSrcProcess, module.UDPConnector):
    def __init__(self, *args):
        super().__init__(*args)
        self.socket = None
        self.timers = {}
        self.gauges = {}
        self.counters = {}
        self.sets = {}
        self.current_timestamp = self.last_timestamp = 0
        # Some of those are illegal in Graphite, so Carbon module has to handle them separately.
        self.metadata_regex = re.compile('^([a-zA-Z][a-zA-Z0-9_]*)[:=]([a-zA-Z0-9_:=\-\+\@\?\#\.\/\%\<\>\*\;\&\[\]]+)$', re.ASCII)

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
        self.current_timestamp = self.last_timestamp = module.monotonic_time()
        while True:
            try:
                self.socket = self.socket or self.get_udp_socket(bind=True)
                data, addr = self.socket.recvfrom(65535)
                self.handle_packet(data, addr)
            except InterruptedError:
                pass

    def enqueue_timers(self, system_timestamp):
        interval = self.current_timestamp - self.last_timestamp
        timeout = self.cfg['timers_timeout']
        for k, (recv_timestamp, cust_timestamp, v) in tuple(self.timers.items()):
            if system_timestamp - recv_timestamp > timeout:
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

                for t in self.cfg.get('percentile_thresholds', ()):
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
                self.buffer.append(
                    (self.cfg['timers_bucket'], timer_stats, cust_timestamp or system_timestamp, dict(k))
                )

            self.timers[k] = recv_timestamp, cust_timestamp, []

    def enqueue_sets(self, system_timestamp):
        timeout = self.cfg['sets_timeout']
        for k, (recv_timestamp, cust_timestamp, v) in tuple(self.sets.items()):
            if system_timestamp - recv_timestamp <= timeout:
                self.buffer.append(
                    (self.cfg['sets_bucket'], {"count": float(len(v))}, cust_timestamp or system_timestamp, dict(k))
                )
                self.sets[k] = recv_timestamp, cust_timestamp, set()
            else:
                del self.sets[k]

    def enqueue_gauges(self, system_timestamp):
        timeout = self.cfg['gauges_timeout']
        for k, (recv_timestamp, cust_timestamp, v) in tuple(self.gauges.items()):
            if system_timestamp - recv_timestamp <= timeout:
                self.buffer.append(
                    (self.cfg['gauges_bucket'], float(v), cust_timestamp or system_timestamp, dict(k))
                )
            else:
                del self.gauges[k]

    def enqueue_counters(self, system_timestamp):
        interval = self.current_timestamp - self.last_timestamp
        timeout = self.cfg['counters_timeout']
        for k, (recv_timestamp, cust_timestamp, v) in tuple(self.counters.items()):
            if system_timestamp - recv_timestamp <= timeout:
                stats = {
                    'rate': float(v) / interval,
                    'count': float(v)
                }
                self.buffer.append(
                    (self.cfg['counters_bucket'], stats, cust_timestamp or system_timestamp, dict(k))
                )
                self.counters[k] = recv_timestamp, cust_timestamp, 0
            else:
                del self.counters[k]

    def handle_packet(self, data, addr=None):
        # Adding a bit of extra sauce so clients can
        # send multiple samples in a single UDP packet.
        try:
            recv_timestamp, data = round(module.system_time(), 3), data.decode("ascii")
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
            recv_timestamp, cust_timestamp, line, metadata = self.handle_metadata(recv_timestamp, line)
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
        key = self.handle_key(name, metadata)
        if not key:
            return

        # I'm not sure if statsd is doing this on purpose
        # but the code allows for name:v1|t1:v2|t2 etc etc.
        # In the interest of compatibility, I'll maintain
        # the behavior.
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
                    self.handle_timer(recv_timestamp, cust_timestamp, key, valstr, ratestr)
                elif typestr == "g":
                    self.handle_gauge(recv_timestamp, cust_timestamp, key, valstr, ratestr)
                elif typestr == "s":
                    self.handle_set(recv_timestamp, cust_timestamp, key, valstr, ratestr)
                else:
                    self.handle_counter(recv_timestamp, cust_timestamp, key, valstr, ratestr)
            except ValueError:
                pass

    def handle_metadata(self, recv_timestamp, line):
        # http://docs.datadoghq.com/guides/dogstatsd/#datagram-format
        bits = line.split("|#", 1)  # We allow '#' in tag values, too
        cust_timestamp, metadata = None, {}
        if len(bits) < 2:
            return recv_timestamp, None, line, metadata
        for i in bits[1].split(","):
            # DataDog docs / examples use key:value, we also handle key=value.
            m = self.metadata_regex.match(i)
            if not m:
                return None, None, None, None
            k, v = m.group(1), m.group(2)
            if k == 'timestamp':
                cust_timestamp = float(v)
                # 2524608000 = secs from epoch to 1 Jan 2050
                if cust_timestamp > 2524608000:
                    cust_timestamp /= 1000
                if cust_timestamp < recv_timestamp - 600 or cust_timestamp > recv_timestamp + 600:
                    raise ValueError()
                cust_timestamp = round(cust_timestamp, 3)
            else:
                metadata[k] = v
        return cust_timestamp or recv_timestamp, cust_timestamp, bits[0], metadata

    def handle_key(self, name, metadata):
        metadata.update(name=name)
        key = tuple((k, metadata[k]) for k in sorted(metadata.keys()))
        return key

    def handle_timer(self, recv_timestamp, cust_timestamp, key, valstr, ratestr):
        val = float(valstr)
        if key in self.timers:
            buf = self.timers[key][2]
            buf.append(val)
            self.timers[key] = recv_timestamp, cust_timestamp, buf
        else:
            self.timers[key] = recv_timestamp, cust_timestamp, [val]

    def handle_gauge(self, recv_timestamp, cust_timestamp, key, valstr, ratestr):
        val = float(valstr)
        delta = valstr[0] in "+-"
        if delta and key in self.gauges:
            self.gauges[key] = recv_timestamp, cust_timestamp, self.gauges[key][2] + val
        else:
            self.gauges[key] = recv_timestamp, cust_timestamp, val

    def handle_set(self, recv_timestamp, cust_timestamp, key, valstr, ratestr):
        if key in self.sets:
            buf = self.sets[key][2]
            buf.add(valstr)
            self.sets[key] = recv_timestamp, cust_timestamp, buf
        else:
            self.sets[key] = recv_timestamp, cust_timestamp, {valstr}

    def handle_counter(self, recv_timestamp, cust_timestamp, key, valstr, ratestr):
        if ratestr and ratestr[0] == "@":
            rate = float(ratestr[1:])
            if rate > 0 and rate <= 1:
                val = float(valstr) / rate
            else:
                return
        else:
            val = float(valstr)
        if key in self.counters:
            val += self.counters[key][2]
        self.counters[key] = recv_timestamp, cust_timestamp, val
