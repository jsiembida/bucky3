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
import string
import signal
import argparse
import importlib
import multiprocessing
import bucky3.cfg as cfg
import bucky3.module as module


MODULES = {
    'carbon_client': ('bucky3.carbon', 'CarbonClient'),
    'influxdb_client': ('bucky3.influxdb', 'InfluxDBClient'),
    'prometheus_exporter': ('bucky3.prometheus', 'PrometheusExporter'),
    'statsd_server': ('bucky3.statsd', 'StatsDServer'),
    'linux_stats': ('bucky3.linuxstats', 'LinuxStatsCollector'),
    'docker_stats': ('bucky3.dockerstats', 'DockerStatsCollector')
}


class Manager(module.Logger):
    def __init__(self, config_file):
        self.config_file = config_file
        self.log = None
        self.src_group = {}
        self.dst_group = {}

    def import_module(self, module_package, module_class):
        m = importlib.import_module(module_package)
        return getattr(m, module_class)

    def load_config(self, config_file):
        new_config = {}
        with open(config_file or cfg.__file__, 'r') as f:
            config_template = string.Template(f.read())
            config_str = config_template.substitute(os.environ)
            exec(config_str, new_config)

        src_modules, dst_modules = [], []

        for k in list(new_config.keys()):
            v = new_config[k]
            if not k.startswith('_') and type(v) == dict and 'module_type' in v:
                module_name, module_type, module_config = k, v['module_type'], new_config.pop(k)
                if module_config.get('module_inactive', False):
                    continue
                module_config['module_inactive'] = False
                if module_type not in MODULES:
                    raise ValueError("Invalid module type %s", module_type)
                module_class = self.import_module(*MODULES[module_type])
                if issubclass(module_class, module.MetricsSrcProcess):
                    src_modules.append((module_name, module_class, module_config))
                elif issubclass(module_class, module.MetricsDstProcess):
                    dst_modules.append((module_name, module_class, module_config))
                else:
                    raise ValueError("Invalid module class %s", module_class)

        if not src_modules:
            raise ValueError("No source modules configured")

        if not dst_modules:
            raise ValueError("No destination modules configured")

        for module_name, module_class, module_config in src_modules + dst_modules:
            for k, v in new_config.items():
                if k not in module_config:
                    module_config[k] = v

        return new_config, src_modules, dst_modules

    def terminate_process(self, proc):
        err = 0
        if proc.exitcode is None:
            self.log.info("Stopping %s", proc.name)
            proc.terminate()
            proc.join(1)
            if proc.exitcode is None:
                self.log.warning("%s still running, killing", proc.name)
                err += 1
                os.kill(proc.pid, 9)
                proc.join()
        else:
            self.log.info("%s has already exited", proc.name)
        return err

    def terminate_group(self, group):
        err = 0
        for module_config, timestamps, proc, args in group.values():
            if proc:
                err += self.terminate_process(proc)
        return err

    def terminate_and_exit(self, err=0):
        err += self.terminate_group(self.src_group)
        err += self.terminate_group(self.dst_group)
        sys.exit(err != 0)

    def start_module(self, module_name, module_class, module_config, timestamps, args, message="Starting %s"):
        timestamps.append(module.monotonic_time())
        timestamps = timestamps[-10:]
        proc = module_class(module_name, module_config, *args)
        self.log.info(message, proc.name)
        proc.start()
        return module_config, timestamps, proc, args

    def healthcheck(self, group):
        err = 0

        for (module_name, module_class), (module_config, timestamps, proc, args) in group.items():
            if proc is None:
                group[(module_name, module_class)] = self.start_module(
                    module_name, module_class, module_config, timestamps, args
                )
            elif proc.exitcode is not None:
                proc.join()
                if len(timestamps) > 5:
                    average_time_between_starts = sum(
                        timestamps[i] - timestamps[i - 1] for i in range(1, len(timestamps))
                    ) / (len(timestamps) - 1)
                    if average_time_between_starts < 60:
                        self.log.critical("%s keeps failing, cannot recover", proc.name)
                        err += 1
                        continue
                if timestamps and (module.monotonic_time() - timestamps[-1]) < 1:
                    self.log.warning("%s has stopped, too early for restart", proc.name)
                else:
                    group[(module_name, module_class)] = self.start_module(
                        module_name, module_class, module_config, timestamps, args, "%s has stopped, restarting"
                    )
            else:
                self.log.debug("%s is up", proc.name)

        return err

    def init(self):
        new_config, src_modules, dst_modules = self.load_config(self.config_file)
        self.log = self.setup_logging(new_config, 'bucky3')
        self.src_group, self.dst_group, pipes = {}, {}, []

        # Using shared pipes leads to data from multiple source modules being occasionally interleaved
        # which means the receiving end tries to unpickle the corrupted stream. So we use N x M pipes.
        for module_name, module_class, module_config in dst_modules:
            tmp = [multiprocessing.Pipe(duplex=False) for i in range(len(src_modules))]
            recv_ends, send_ends = tuple(i[0] for i in tmp), tuple(i[1] for i in tmp)
            self.dst_group[(module_name, module_class)] = module_config, [], None, (recv_ends,)
            pipes.append(send_ends)
        for i, (module_name, module_class, module_config) in enumerate(src_modules):
            send_ends = [j[i] for j in pipes]
            self.src_group[(module_name, module_class)] = module_config, [], None, (send_ends,)

    def termination_handler(self, signal_number, stack_frame):
        self.terminate_and_exit(0)

    def run(self):
        self.init()

        signal.signal(signal.SIGINT, self.termination_handler)
        signal.signal(signal.SIGTERM, self.termination_handler)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        signal.signal(signal.SIGALRM, signal.SIG_IGN)

        while True:
            try:
                err = self.healthcheck(self.dst_group) + self.healthcheck(self.src_group)
                if err:
                    self.terminate_and_exit(err)
                module.sleep(3)
            except InterruptedError:
                pass


def main(argv=sys.argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file", help="An optional config file", nargs='?', default=None)
    args = parser.parse_args(argv[1:])
    Manager(args.config_file).run()


if __name__ == '__main__':
    main()
