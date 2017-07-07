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
import sys
import time
import multiprocessing
import bucky.cfg as cfg
import bucky.common as common
import bucky.carbon as carbon
import bucky.statsd as statsd
import bucky.influxdb as influxdb
import bucky.prometheus as prometheus
import bucky.systemstats as systemstats
import bucky.dockerstats as dockerstats


MODULES = {
    'carbon_client': carbon.CarbonClient,
    'influxdb_client': influxdb.InfluxDBClient,
    'prometheus_exporter': prometheus.PrometheusExporter,
    'statsd_server': statsd.StatsDServer,
    'system_stats': systemstats.SystemStatsCollector,
    'docker_stats': dockerstats.DockerStatsCollector
}


def main(argv=sys.argv):
    if len(argv) == 1:
        config_file = None
    elif len(argv) == 2:
        config_file = argv[1]
    else:
        # TODO show a meaningful message
        print("Error...", file=sys.stderr)
        sys.exit(1)

    src_subprocesses, dst_subprocesses = {}, {}

    def shutdown(subprocesses):
        err = 0
        for timestamps, p, arg in subprocesses.values():
            if p and p.exitcode is None:
                cfg.log.info("Terminating module %s", p.name)
                p.terminate()
        for timestamps, p, arg in subprocesses.values():
            p.join(1)
            if p.exitcode is None:
                cfg.log.warning("Module %s still running, killing", p.name)
                err = 1
                os.kill(p.pid, 9)
                p.join()
        return err

    def healthcheck(subprocesses):
        def start_module(module_name, module_class, timestamps, arg):
            now = time.time()
            timestamps.append(now)
            timestamps = timestamps[-10:]
            p = module_class(module_name, config_file, arg)
            cfg.log.info("Starting module %s", p.name)
            p.start()
            return timestamps, p, arg

        err = 0

        for (module_name, module_class), (timestamps, p, arg) in subprocesses.items():
            if p is None:
                subprocesses[(module_name, module_class)] = start_module(module_name, module_class, timestamps, arg)
                continue
            if p.exitcode is not None:
                cfg.log.warning("Module %s has exited, trying to recover", p.name)
                p.join()
                if len(timestamps) > 5:
                    average_time_between_starts = sum(
                        timestamps[i] - timestamps[i - 1] for i in range(1, len(timestamps))
                    ) / (len(timestamps) - 1)
                    if average_time_between_starts < 60:
                        cfg.log.critical("Module %s keeps failing, cannot recover", p.name)
                        err = 1
                        continue
                subprocesses[(module_name, module_class)] = start_module(module_name, module_class, timestamps, arg)
                continue
            cfg.log.debug("Module %s is up", p.name)

        return err

    def terminate(err=0):
        err += shutdown(src_subprocesses)
        err += shutdown(dst_subprocesses)
        sys.exit(err != 0)

    common.prepare_module('bucky3', config_file, lambda: True, terminate)

    src_buf, dst_buf = [], []

    for k, v in vars(cfg).items():
        if not k.startswith('_') and type(v) == dict and 'type' in v:
            module_name, module_type = k, v['type']
            module_class = MODULES[module_type]
            if issubclass(module_class, common.MetricsSrcProcess):
                src_buf.append((module_name, module_class))
            elif issubclass(module_class, common.MetricsDstProcess):
                dst_buf.append((module_name, module_class))
            else:
                raise ValueError("Invalid module type")

    pipes = []
    for module_name, module_class in dst_buf:
        send, recv = multiprocessing.Pipe()
        dst_subprocesses[(module_name, module_class)] = [], None, recv
        pipes.append(send)
    for module_name, module_class in src_buf:
        src_subprocesses[(module_name, module_class)] = [], None, pipes

    while True:
        err = healthcheck(src_subprocesses) + healthcheck(dst_subprocesses)
        if err:
            terminate(err)
        time.sleep(10)


if __name__ == '__main__':
    main()
