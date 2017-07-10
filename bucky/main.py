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
import signal
import multiprocessing
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

    log, src_group, dst_group = None, {}, {}

    def terminate(group):
        err = 0
        for timestamps, p, args in group.values():
            if p:
                if p.exitcode is None:
                    log.info("Stopping %s", p.name)
                    p.terminate()
                    p.join(1)
                    if p.exitcode is None:
                        log.warning("%s still running, killing", p.name)
                        err += 1
                        os.kill(p.pid, 9)
                        p.join()
                else:
                    log.info("%s has already exited", p.name)
        return err

    def terminate_and_exit(err=0):
        err += terminate(src_group)
        err += terminate(dst_group)
        sys.exit(err != 0)

    def healthcheck(group):
        def start(module_name, module_class, timestamps, args, message="Starting %s"):
            timestamps.append(time.monotonic())
            timestamps = timestamps[-10:]
            p = module_class(module_name, config_file, *args)
            log.info(message, p.name)
            p.start()
            return timestamps, p, args

        err = 0

        for (module_name, module_class), (timestamps, p, args) in group.items():
            if p is None:
                group[(module_name, module_class)] = start(module_name, module_class, timestamps, args)
            elif p.exitcode is not None:
                p.join()
                if len(timestamps) > 5:
                    average_time_between_starts = sum(
                        timestamps[i] - timestamps[i - 1] for i in range(1, len(timestamps))
                    ) / (len(timestamps) - 1)
                    if average_time_between_starts < 60:
                        log.critical("%s keeps failing, cannot recover", p.name)
                        err += 1
                        continue
                if timestamps and (time.monotonic() - timestamps[-1]) < 1:
                    log.warning("%s has stopped, too early for restart", p.name)
                else:
                    group[(module_name, module_class)] = start(
                        module_name, module_class, timestamps, args, "%s has stopped, restarting"
                    )
            else:
                log.debug("%s is up", p.name)

        return err

    def prepare_modules(cfg):
        src_buf, dst_buf = [], []

        for k, v in cfg.items():
            if not k.startswith('_') and type(v) == dict and 'module_type' in v:
                module_name, module_type = k, v['module_type']
                module_class = MODULES[module_type]
                if issubclass(module_class, common.MetricsSrcProcess):
                    src_buf.append((module_name, module_class))
                elif issubclass(module_class, common.MetricsDstProcess):
                    dst_buf.append((module_name, module_class))
                else:
                    raise ValueError("Invalid module type")

        src, dst, pipes = {}, {}, []

        for module_name, module_class in dst_buf:
            send, recv = multiprocessing.Pipe()
            dst[(module_name, module_class)] = [], None, (recv,)
            pipes.append(send)
        for module_name, module_class in src_buf:
            src[(module_name, module_class)] = [], None, (pipes,)

        return src, dst

    def termination_handler(signal_number, stack_frame):
        terminate_and_exit(0)

    def restart_handler(signal_number, stack_frame):
        nonlocal log, src_group, dst_group
        terminate(src_group)
        terminate(dst_group)
        cfg = common.load_config(config_file)
        log = common.setup_logging(cfg, 'bucky3')
        src_group, dst_group = prepare_modules(cfg)

    signal.signal(signal.SIGINT, termination_handler)
    signal.signal(signal.SIGTERM, termination_handler)
    signal.signal(signal.SIGHUP, restart_handler)

    restart_handler(None, None)
    while True:
        err = healthcheck(src_group) + healthcheck(dst_group)
        if err:
            terminate_and_exit(err)
        time.sleep(3)


if __name__ == '__main__':
    main()
