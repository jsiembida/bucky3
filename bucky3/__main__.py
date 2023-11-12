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
import string
import signal
import argparse
import importlib
import multiprocessing
import bucky3.cfg as cfg
import bucky3.module as module


MODULES = {
    'influxdb_client': ('bucky3.influxdb', 'InfluxDBClient'),
    'elasticsearch_client': ('bucky3.elasticsearch', 'ElasticsearchClient'),
    'prometheus_exporter': ('bucky3.prometheus', 'PrometheusExporter'),
    'statsd_server': ('bucky3.statsd', 'StatsDServer'),
    'jsond_server': ('bucky3.jsond', 'JsonDServer'),
    'linux_stats': ('bucky3.linux', 'LinuxStatsCollector'),
    'docker_stats': ('bucky3.docker', 'DockerStatsCollector'),
    'systemd_journal': ('bucky3.journal', 'SystemdJournal'),
    'debug_output': ('bucky3.debug', 'DebugOutput'),
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
            if not k.startswith('_') and isinstance(v, dict) and 'module_type' in v:
                module_name, module_type, module_config = k, v['module_type'], new_config.pop(k)
                if module_config.get('module_inactive', False):
                    continue
                if module_type not in MODULES:
                    raise ValueError("Invalid module type %s", module_type)
                module_class = self.import_module(*MODULES[module_type])
                if issubclass(module_class, module.MetricsSrcProcess):
                    src_modules.append((module_name, module_class, module_config))
                elif issubclass(module_class, module.MetricsDstProcess):
                    dst_modules.append((module_name, module_class, module_config))

        for module_name, module_class, module_config in src_modules + dst_modules:
            for k, v in new_config.items():
                if k not in module_config:
                    module_config[k] = v

        for module_name, module_class, module_config in src_modules:
            destination_modules = module_config.get('destination_modules')
            if destination_modules:
                tmp = []
                for m in destination_modules:
                    if isinstance(m, str):
                        found_destinations = tuple(filter(lambda i: i[0] == m, dst_modules))
                    else:
                        found_destinations = tuple(filter(lambda i: id(i[2]) == id(m), dst_modules))
                    if found_destinations:
                        tmp.append(found_destinations[0])
                    else:
                        raise ValueError("No configured destination found for " + module_name)
                module_config['destination_modules'] = tmp
            else:
                module_config['destination_modules'] = dst_modules

        return new_config, src_modules, dst_modules

    def terminate_group(self, group):
        procs = []
        for module_config, timestamps, proc, args in group.values():
            if not proc:
                continue
            if proc.exitcode is None:
                self.log.info("Stopping %s", proc.name)
                proc.terminate()
                procs.append(proc)
            else:
                self.log.info("%s has already exited", proc.name)

        err = 0
        for proc in procs:
            proc.join(5)
            if proc.exitcode is None:
                self.log.warning("%s still running, killing", proc.name)
                err += 1
                os.kill(proc.pid, 9)
                proc.join()

        return err

    def terminate_and_exit(self, err=0):
        err += self.terminate_group(self.src_group)
        err += self.terminate_group(self.dst_group)
        sys.exit(err != 0)

    def start_module(self, module_name, module_class, module_config, timestamps, args, message="Starting %s"):
        timestamps.append(time.monotonic())
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
                if timestamps and (time.monotonic() - timestamps[-1]) < 1:
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
        self.log = self.init_log(new_config, 'bucky3')
        self.src_group, self.dst_group, pipes = {}, {}, []

        # Using shared pipes leads to data from multiple source modules being occasionally interleaved
        # which means the receiving end tries to unpickle the corrupted stream. So we use N x M pipes.
        recv_ends = {}
        for module_name, module_class, module_config in src_modules:
            send_ends = []
            for dst_module in module_config['destination_modules']:
                recv_end, send_end = multiprocessing.Pipe(duplex=False)
                send_ends.append(send_end)
                recv_ends.setdefault(dst_module[0], []).append(recv_end)
            self.src_group[(module_name, module_class)] = module_config, [], None, (send_ends,)
        for module_name, module_class, module_config in dst_modules:
            self.dst_group[(module_name, module_class)] = module_config, [], None, (recv_ends[module_name],)

    def termination_handler(self, signal_number, stack_frame):
        self.terminate_and_exit(0)

    def run(self):
        self.init()

        signal.signal(signal.SIGINT, self.termination_handler)
        signal.signal(signal.SIGTERM, self.termination_handler)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)

        while True:
            err = self.healthcheck(self.dst_group) + self.healthcheck(self.src_group)
            if err:
                self.terminate_and_exit(err)
            time.sleep(3)


def main(argv=sys.argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("cfg_file", help="An optional cfg file", nargs='?', default=None)
    args = parser.parse_args(argv[1:])
    Manager(args.cfg_file).run()


if __name__ == '__main__':
    main()
