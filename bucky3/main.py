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
import signal
import importlib
import multiprocessing
import bucky3.common as common
import bucky3.module as module


MODULES = {
    'carbon_client': ('bucky3.carbon', 'CarbonClient'),
    'influxdb_client': ('bucky3.influxdb', 'InfluxDBClient'),
    'prometheus_exporter': ('bucky3.prometheus', 'PrometheusExporter'),
    'statsd_server': ('bucky3.statsd', 'StatsDServer'),
    'linux_stats': ('bucky3.linuxstats', 'LinuxStatsCollector'),
    'docker_stats': ('bucky3.dockerstats', 'DockerStatsCollector')
}


class Manager:
    def __init__(self, config_file):
        self.config_file = config_file
        self.log = None
        self.src_group = {}
        self.dst_group = {}

    def terminate_module(self, p):
        err = 0
        if p.exitcode is None:
            self.log.info("Stopping %s", p.name)
            p.terminate()
            p.join(1)
            if p.exitcode is None:
                self.log.warning("%s still running, killing", p.name)
                err += 1
                os.kill(p.pid, 9)
                p.join()
        else:
            self.log.info("%s has already exited", p.name)
        return err

    def terminate_group(self, group):
        err = 0
        for timestamps, p, args in group.values():
            if p:
                err += self.terminate_module(p)
        return err

    def terminate_and_exit(self, err=0):
        err += self.terminate_group(self.src_group)
        err += self.terminate_group(self.dst_group)
        sys.exit(err != 0)

    def start_module(self, module_name, module_class, timestamps, args, message="Starting %s"):
        timestamps.append(module.monotonic_time())
        timestamps = timestamps[-10:]
        p = module_class(module_name, self.config_file, *args)
        self.log.info(message, p.name)
        p.start()
        return timestamps, p, args

    def healthcheck(self, group):
        err = 0

        for (module_name, module_class), (timestamps, p, args) in group.items():
            if p is None:
                group[(module_name, module_class)] = self.start_module(module_name, module_class, timestamps, args)
            elif p.exitcode is not None:
                p.join()
                if len(timestamps) > 5:
                    average_time_between_starts = sum(
                        timestamps[i] - timestamps[i - 1] for i in range(1, len(timestamps))
                    ) / (len(timestamps) - 1)
                    if average_time_between_starts < 60:
                        self.log.critical("%s keeps failing, cannot recover", p.name)
                        err += 1
                        continue
                if timestamps and (module.monotonic_time() - timestamps[-1]) < 1:
                    self.log.warning("%s has stopped, too early for restart", p.name)
                else:
                    group[(module_name, module_class)] = self.start_module(
                        module_name, module_class, timestamps, args, "%s has stopped, restarting"
                    )
            else:
                self.log.debug("%s is up", p.name)

        return err

    def import_module(self, module_package, module_class):
        importlib.invalidate_caches()
        m = importlib.import_module(module_package)
        return getattr(m, module_class)

    def prepare_modules(self, cfg):
        src_buf, dst_buf = [], []

        for k, v in cfg.items():
            if not k.startswith('_') and type(v) == dict and 'module_type' in v:
                module_name, module_type = k, v['module_type']
                if module_type not in MODULES:
                    raise ValueError("Invalid module type %s", module_type)
                module_class = self.import_module(*MODULES[module_type])
                if issubclass(module_class, module.MetricsSrcProcess):
                    src_buf.append((module_name, module_class))
                elif issubclass(module_class, module.MetricsDstProcess):
                    dst_buf.append((module_name, module_class))
                else:
                    raise ValueError("Invalid module class %s", module_class)

        src, dst, pipes = {}, {}, []

        for module_name, module_class in dst_buf:
            send, recv = multiprocessing.Pipe()
            dst[(module_name, module_class)] = [], None, (recv,)
            pipes.append(send)
        for module_name, module_class in src_buf:
            src[(module_name, module_class)] = [], None, (pipes,)

        return src, dst

    def termination_handler(self, signal_number, stack_frame):
        self.terminate_and_exit(0)

    def restart_handler(self, signal_number, stack_frame):
        self.terminate_group(self.src_group)
        self.terminate_group(self.dst_group)
        cfg = common.load_config(self.config_file)
        self.log = common.setup_logging(cfg, 'bucky3')
        self.src_group, self.dst_group = self.prepare_modules(cfg)

    def run(self):
        self.restart_handler(None, None)

        signal.signal(signal.SIGINT, self.termination_handler)
        signal.signal(signal.SIGTERM, self.termination_handler)
        signal.signal(signal.SIGHUP, self.restart_handler)

        while True:
            try:
                err = self.healthcheck(self.src_group) + self.healthcheck(self.dst_group)
                if err:
                    self.terminate_and_exit(err)
                module.sleep(3)
            except InterruptedError:
                pass


def main(argv=sys.argv):
    if len(argv) == 1:
        config_file = None
    elif len(argv) == 2:
        config_file = argv[1]
    else:
        print("\n\nOne optional argument is accepted, path to config file.\n\n", file=sys.stderr)
        sys.exit(1)

    Manager(config_file).run()


if __name__ == '__main__':
    main()
