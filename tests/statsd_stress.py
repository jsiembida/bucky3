#!/usr/bin/env python3


"""

This is a script emulating a stream of statsd metrics coming from an application.


Example run:

./statsd_stress.py 100 30 http_timing ms zlib \
    "host=$(hostname)"                        \
    "app=webapp"                              \
    "env=prod"                                \
    "http_method=GET"                         \
    "http_path=/api/v?/call?"                 \
    "http_code=?00"                           \
    "commit=5f65cc7f856c"

100 - max interval between batches, in ms, uniformly randomized.
      Meaning, this command will send a batch on average every 50ms.
20 - max batch size, uniformly randomized.
     Meaning, batches will on average have 10 metrics each.
http_timing - metric name used.
ms - type of metrics produced.
* - k=v pairs used for each sent metric. Question marks produce single digit permutations.
    http_path=/api/v?/call? - results in a set of 100 k=v pairs.


For a more realistic workload, you want to run multiple such commands with different configurations.

"""


import sys
import time
import socket
import random
import zlib
import gzip


def permute_args(argv):
    def permute(s):
        i = s.find('?')
        if i < 0:
            yield s
        else:
            prefix, suffix = s[:i], s[i + 1:]
            for c in '0123456789':
                yield from permute(prefix + c + suffix)

    for arg in argv:
        yield tuple(permute(arg))


def random_metric(metric_name, metric_type, metric_metadata):
    random_value = round(10 * random.random(), 2)
    random_metadata = [random.choice(i) for i in metric_metadata]
    return metric_name + ':' + str(random_value) + '|' + metric_type + '|#' + ','.join(random_metadata)


def main(argv):
    max_sleep_ms = int(argv[1])
    max_batch_size = int(argv[2])
    metric_name = argv[3]
    metric_type = argv[4]
    args = argv[5:]
    compress = lambda b: b
    if args[0] == 'zlib':
        compress = zlib.compress
        args = args[1:]
    elif args[0] == 'gzip':
        compress = gzip.compress
        args = args[1:]
    metric_metadata = tuple(permute_args(args))
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    last_update = time.time()
    metric_counter = 0
    packet_counter = 0
    bytes_counter = 0

    target = ('127.0.0.1', 8125)

    while True:
        batch_size = max(random.randint(0, max_batch_size), 1)
        batch_lines = [
            random_metric(metric_name, metric_type, metric_metadata)
            for _ in range(batch_size)
        ]
        batch_packet = compress('\n'.join(batch_lines).encode('ascii'))
        sock.sendto(batch_packet, target)
        metric_counter += batch_size
        bytes_counter += len(batch_packet)
        packet_counter += 1
        sleep_ms = min(max(random.randint(0, max_sleep_ms), 20), 1000)
        time.sleep(sleep_ms / 1000)
        now = time.time()
        if now - last_update >= 10:
            time_diff = now - last_update
            metrics_ps = metric_counter / time_diff
            packets_ps = packet_counter / time_diff
            bytes_ps = bytes_counter / time_diff
            print('{:d} metrics in {:.1f} seconds = {:.1f} metrics/s, {:.1f} packets/s, {:.1f} bytes/s'.format(
                metric_counter, now - last_update, metrics_ps, packets_ps, bytes_ps
            ), file=sys.stderr)
            last_update = time.time()
            metric_counter = 0
            packet_counter = 0
            bytes_counter = 0


if __name__ == '__main__':
    main(sys.argv)
