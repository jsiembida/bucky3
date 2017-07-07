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

    source_subprocesses = []
    destination_subprocesses = []

    def shutdown(subprocesses):
        err = 0
        for i in subprocesses:
            cfg.log.info("Terminating %s", i.name)
            i.terminate()
        for i in subprocesses:
            i.join(1)
            if i.exitcode is None:
                cfg.log.warning("%s still running, killing", i.name)
                err = 1
                os.kill(i.pid, 9)
                i.join()
        return err

    def startup(subprocesses):
        for i in subprocesses:
            cfg.log.info("Starting %s", i.name)
            i.start()

    def terminate():
        err = shutdown(source_subprocesses)
        err += shutdown(destination_subprocesses)
        sys.exit(err != 0)

    def tick():
        pass

    common.prepare_module('bucky3', config_file, tick, terminate)

    sources = []
    destinations = []

    for k, v in vars(cfg).items():
        if not k.startswith('_') and type(v) == dict and 'type' in v:
            module_name, module_type = k, v['type']
            module_class = MODULES[module_type]
            if issubclass(module_class, common.MetricsSrcProcess):
                sources.append((module_name, module_class))
            elif issubclass(module_class, common.MetricsDstProcess):
                destinations.append((module_name, module_class))
            else:
                raise ValueError("Invalid module type")

    pipes = []
    for module_name, module_class in destinations:
        send, recv = multiprocessing.Pipe()
        destination_subprocesses.append(module_class(module_name, config_file, recv))
        pipes.append(send)
    for module_name, module_class in sources:
        source_subprocesses.append(module_class(module_name, config_file, pipes))

    startup(destination_subprocesses)
    startup(source_subprocesses)

    while True:
        time.sleep(10)


if __name__ == '__main__':
    main()
