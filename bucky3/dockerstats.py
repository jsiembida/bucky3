
"""
We need meta info (names, labels, etc) about the containers as well as their resource
consumption. Docker-py has all necessary calls, but the low level stats call itself:
   docker-py.readthedocs.io/en/stable/api.html#docker.api.container.ContainerApiMixin.stats
is slow and unpredictable at that. It can take anything from 1 to 3 secs to collect
stats for one container. For many containers and short flush_intervals we got a problem.
The call can be parallelized but that leads to clunky multithreaded implementation that
scales only this much (say, 30 threads could handle 100 containers for flush_interval=10).

Hence this hybrid implementation. It extracts the resources usage from /sys & /proc but
gets the meta info via docker API calls (those used are fast though). Consequently:
- it only works with local docker (because it uses unix socket)
- it only works locally on linux (because it uses /proc & /sys)
- it is single threaded and fast, metrics for 100 containers are collected in a split of sec
- it has no external dependencies, vanilla Python3
- it is vulnerable to future changes in docker API as well as changes in /sys layout
"""

import re
import json
import socket
import http.client
import bucky3.module as module


class DockerConnection(http.client.HTTPConnection):
    def __init__(self, docker_socket, api_version):
        super().__init__(docker_socket)
        self.api_version = api_version

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.host)

    def _get_json(self, url):
        url = '/v' + self.api_version + url
        self.request('GET', url)
        resp = self.getresponse()
        if resp.status != 200:
            raise ConnectionError('Docker error code {} for {}'.format(resp.status, url))
        return json.loads(resp.read().decode('utf-8'))

    def list_containers(self):
        return self._get_json('/containers/json')

    def inspect_container(self, container_id):
        return self._get_json('/containers/' + str(container_id) + '/json?size=true')


class DockerStatsCollector(module.MetricsSrcProcess, module.ProcfsReader):
    def __init__(self, *args):
        super().__init__(*args)
        # See the statsd metadata matching regexp.
        self.env_regex = re.compile('^([a-zA-Z][a-zA-Z0-9_]*)=([a-zA-Z0-9_:=\-\+\@\?\#\.\/\%\<\>\*\;\&\[\]]+)$', re.ASCII)

    def init_config(self):
        super().init_config()
        self.api_version = self.cfg.get('api_version', '1.22')
        self.docker_socket = self.cfg.get('docker_socket', '/var/run/docker.sock')
        self.env_mapping = self.cfg.get('env_mapping')
        self.system_memory = dict(self.read_memory())['total_bytes']

    def read_df_stats(self, buffer, container_id, timestamp, container_metadata, inspect_info):
        df_stats = {
            'total_bytes': int(inspect_info['SizeRootFs']),
            'used_bytes': int(inspect_info.get('SizeRw', 0)),
        }
        buffer.append(("docker_filesystem", df_stats, timestamp, container_metadata))

    def read_cpu_stats(self, buffer, container_id, timestamp, container_metadata, inspect_info):
        host_config = inspect_info['HostConfig']
        with open('/sys/fs/cgroup/cpu/docker/' + container_id + '/cpuacct.usage_percpu') as f:
            cpu_tokens = f.read().strip().split()
            for k, v in enumerate(cpu_tokens):
                metadata = container_metadata.copy()
                metadata.update(name='cpu' + str(k))
                buffer.append(("docker_cpu", {'usage': int(v)}, timestamp, metadata))
            # Docker reports CPU counters in nanosecs but quota/period in microsecs, here we make sure we send out
            # all CPU metrics in nanosecs - that differs from linuxstats module CPU counters that are in USER_HZ.
            limit_ps = host_config.get('NanoCpus', 0)
            if not limit_ps:
                cpu_period = host_config.get('CpuPeriod', 0) or 1000000
                cpu_quota = host_config.get('CpuQuota', 0)
                if not cpu_quota:
                    cpu_quota = cpu_period * len(cpu_tokens)
                limit_ps = round(1000000000 * cpu_quota / cpu_period)
            buffer.append(("docker_cpu", {'limit_ps': limit_ps}, timestamp, container_metadata))

    def read_interface_stats(self, buffer, container_id, timestamp, container_metadata, inspect_info):
        root_pid = inspect_info['State']['Pid']
        for interface_name, interface_stats in self.read_interfaces('/proc/' + str(root_pid) + '/net/dev'):
            metadata = container_metadata.copy()
            metadata.update(name=interface_name)
            buffer.append(("docker_interface", interface_stats, timestamp, metadata))

    def read_memory_stats(self, buffer, container_id, timestamp, container_metadata, inspect_info):
        host_config = inspect_info['HostConfig']
        with open('/sys/fs/cgroup/memory/docker/' + container_id + '/memory.usage_in_bytes') as f:
            buffer.append(("docker_memory", {
                'used_bytes': int(f.read().strip()),
                'limit_bytes': int(host_config.get('Memory') or self.system_memory),
            }, timestamp, container_metadata))

    def extract_metadata(self, container_id, container_info, inspect_info):
        inspect_config = inspect_info['Config']
        container_metadata = {}
        if self.env_mapping:
            for l in inspect_config.get('Env', []):
                m = self.env_regex.match(l)
                if m:
                    env_name, env_value = m.group(1), m.group(2)
                    if env_name in self.env_mapping:
                        container_metadata[self.env_mapping[env_name]] = env_value
        if container_info.get('Names'):
            container_metadata['docker_name'] = container_info['Names'][0]
        container_metadata.update(inspect_config.get('Labels', {}))
        container_metadata['docker_id'] = container_id[:12]
        return container_metadata

    def flush(self, system_timestamp):
        timestamp = system_timestamp if self.add_timestamps else None
        docker_connection = None
        try:
            self.log.debug('Starting containers scan')
            docker_connection = DockerConnection(self.docker_socket, self.api_version)
            for container_info in sorted(docker_connection.list_containers(), key=lambda v: v.get('Id')):
                try:
                    buffer = []
                    container_id = container_info['Id']
                    inspect_info = docker_connection.inspect_container(container_id)
                    container_metadata = self.extract_metadata(container_id, container_info, inspect_info)
                    self.read_df_stats(buffer, container_id, timestamp, container_metadata, inspect_info)
                    self.read_cpu_stats(buffer, container_id, timestamp, container_metadata, inspect_info)
                    self.read_memory_stats(buffer, container_id, timestamp, container_metadata, inspect_info)
                    self.read_interface_stats(buffer, container_id, timestamp, container_metadata, inspect_info)
                    for metric in buffer:
                        self.buffer_metric(*metric)
                except FileNotFoundError:
                    pass
            self.log.debug('Finished containers scan')
            return super().flush(system_timestamp)
        except (ConnectionError, FileNotFoundError):
            self.log.exception("Docker error, is it running?")
            super().flush(system_timestamp)
            return False
        finally:
            if docker_connection:
                docker_connection.close()
