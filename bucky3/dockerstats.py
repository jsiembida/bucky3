

import re
import docker
import requests.exceptions
import bucky3.module as module


class DockerStatsCollector(module.MetricsSrcProcess):
    def __init__(self, *args):
        super().__init__(*args)
        # See the statsd metadata matching regexp.
        self.env_regex = re.compile('^([a-zA-Z][a-zA-Z0-9_]*)=([a-zA-Z0-9_:=\-\+\@\?\#\.\/\%\<\>\*\;\&\[\]]+)$', re.ASCII)

    def init_config(self):
        super().init_config()
        api_version = self.cfg.get('api_version')
        self.env_mapping = self.cfg.get('env_mapping')
        self.docker_client = docker.client.from_env(version=api_version)

    def read_df_stats(self, timestamp, container_metadata, total_size, rw_size):
        docker_df_stats = {
            'total_bytes': int(total_size),
            'used_bytes': int(rw_size)
        }
        self.buffer.append(("docker_filesystem", docker_df_stats, timestamp, container_metadata))

    def read_cpu_stats(self, timestamp, container_metadata, stats, host_config):
        # For some reason, we get an occasional KeyError for percpu_usage, hence the extra check.
        if 'percpu_usage' not in stats:
            return
        cpu_stats = stats['percpu_usage']
        # Docker reports CPU counters in nanosecs but quota/period in microsecs, here we make sure we send out
        # all CPU metrics in nanosecs - that differs from linuxstats module CPU counters that are in USER_HZ.
        limit_ps = host_config.get('NanoCpus', 0)
        if not limit_ps:
            cpu_period = host_config.get('CpuPeriod', 0) or 1000000
            cpu_quota = host_config.get('CpuQuota', 0)
            if not cpu_quota:
                cpu_quota = cpu_period * len(cpu_stats)
            limit_ps = round(1000000000 * cpu_quota / cpu_period)
        self.buffer.append(("docker_cpu", {'limit_ps': limit_ps}, timestamp, container_metadata.copy()))
        for k, v in enumerate(cpu_stats):
            metadata = container_metadata.copy()
            metadata.update(name=k)
            self.buffer.append(("docker_cpu", {'usage': int(v)}, timestamp, metadata))

    def read_interface_stats(self, timestamp, container_metadata, stats):
        if not stats:
            # This can happen if i.e. the container was started with --network="host"
            return
        keys = (
            'rx_bytes', 'rx_packets', 'rx_errors', 'rx_dropped',
            'tx_bytes', 'tx_packets', 'tx_errors', 'tx_dropped'
        )
        for k, v in stats.items():
            metadata = container_metadata.copy()
            metadata.update(name=k)
            docker_interface_stats = {k: int(v[k]) for k in keys}
            self.buffer.append(("docker_interface", docker_interface_stats, timestamp, metadata))

    def read_memory_stats(self, timestamp, container_metadata, stats):
        self.buffer.append(("docker_memory", {
            'used_bytes': int(stats['usage']),
            'limit_bytes': int(stats['limit']),
        }, timestamp, container_metadata))

    def get_container_info(self, container):
        container_id = container['Id']
        inspect_info = self.docker_client.api.inspect_container(container_id)
        inspect_config = inspect_info['Config']
        host_config = inspect_info['HostConfig']

        container_metadata = {}
        if self.env_mapping:
            for l in inspect_config.get('Env', []):
                m = self.env_regex.match(l)
                if m:
                    env_name, env_value = m.group(1), m.group(2)
                    if env_name in self.env_mapping:
                        container_metadata[self.env_mapping[env_name]] = env_value
        if container.get('Names'):
            container_metadata['docker_name'] = container['Names'][0]
        container_metadata.update(inspect_config.get('Labels', {}))
        container_metadata['docker_id'] = container_id[:12]
        return container_id, container_metadata, host_config

    def flush(self, monotonic_timestamp, system_timestamp):
        try:
            for i, container in enumerate(self.docker_client.api.containers(size=True)):
                if container.get('State') == 'running' or container.get('Status', '').startswith('Up'):
                    container_id, container_metadata, host_config = self.get_container_info(container)
                    stats_info = self.docker_client.api.stats(container_id, decode=True, stream=False)
                    self.read_df_stats(system_timestamp, container_metadata, int(container['SizeRootFs']), int(container.get('SizeRw', 0)))
                    self.read_cpu_stats(system_timestamp, container_metadata, stats_info['cpu_stats']['cpu_usage'], host_config)
                    self.read_memory_stats(system_timestamp, container_metadata, stats_info['memory_stats'])
                    self.read_interface_stats(system_timestamp, container_metadata, stats_info.get('networks'))
            return super().flush(monotonic_timestamp, system_timestamp)
        except requests.exceptions.ConnectionError:
            self.log.info("Docker connection error, is docker running?")
            super().flush(monotonic_timestamp, system_timestamp)
            return False
        except ValueError:
            self.log.exception("Docker error")
            super().flush(monotonic_timestamp, system_timestamp)
            return False
