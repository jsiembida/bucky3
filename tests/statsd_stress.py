#!/usr/bin/env python3


"""

This is a script emulating a stream of statsd metrics coming from an application.
Timer/histogram metrics are heaviest for statsd to process, so only those are sent.


Example run:

./statsd_stress.py 100 20 http_timing \
    "host=$(hostname)"                \
    "app=webapp"                      \
    "env=prod"                        \
    "http_method=GET"                 \
    "http_path=/api/v?/call?"         \
    "http_code=200"                   \
    "commit=5f65cc7f856c"

100 - max interval between batches, in ms, uniformly randomized.
      Meaning, this command will send a batch on average every 50ms.
20 - max batch size, uniformly randomized.
     Meaning, batches will on average have 10 metrics each.
http_timing - metric name used.
* - k=v pairs used for each sent metric. Question marks produce single digit permutations.
    http_path=/api/v?/call? - results in a set of 100 k=v pairs.


For a more realistic workload, you want to run multiple such commands with different configurations.

"""


import sys
import time
import socket
import random


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


def random_metric(metric_name, metric_metadata):
    random_value = round(10 * random.random(), 2)
    random_metadata = [random.choice(i) for i in metric_metadata]
    return metric_name + ':' + str(random_value) + '|ms|#' + ','.join(random_metadata)


def main(argv):
    max_sleep_ms = int(argv[1])
    max_batch_size = int(argv[2])
    metric_name = argv[3]
    metric_metadata = tuple(permute_args(argv[4:]))
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    last_update = time.time()
    metric_counter = 0

    while True:
        batch_size = max(random.randint(0, max_batch_size), 1)
        for i in range(batch_size):
            metric_line = random_metric(metric_name, metric_metadata)
            sock.sendto(metric_line.encode('ascii'), ('127.0.0.1', 8125))
        metric_counter += batch_size
        sleep_ms = min(max(random.randint(0, max_sleep_ms), 20), 1000)
        time.sleep(sleep_ms / 1000)
        now = time.time()
        if now - last_update >= 10:
            metrics_ps = metric_counter / (now - last_update)
            print('{:d} metrics in {:.1f} seconds = {:.1f} metrics / s'.format(
                metric_counter, now - last_update, metrics_ps
            ), file=sys.stderr)
            last_update = time.time()
            metric_counter = 0


if __name__ == '__main__':
    main(sys.argv)
