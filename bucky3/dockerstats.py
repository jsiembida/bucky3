

import docker
import requests.exceptions
import bucky3.module as module


class DockerStatsCollector(module.MetricsSrcProcess):
    def __init__(self, *args):
        super().__init__(*args)

    def init_config(self):
        super().init_config()
        api_version = self.cfg.get('api_version', None)
        self.docker_client = docker.client.from_env(version=api_version)

    def read_df_stats(self, timestamp, labels, total_size, rw_size):
        docker_df_stats = {
            'total_bytes': int(total_size),
            'used_bytes': int(rw_size)
        }
        self.buffer.append(("docker_filesystem", docker_df_stats, timestamp, labels))

    def read_cpu_stats(self, timestamp, labels, stats, host_config):
        # For some reason, we get an occasional KeyError for percpu_usage, hence the extra check.
        if 'percpu_usage' not in stats:
            return
        cpu_stats = stats['percpu_usage']
        # Docker reports CPU counters in nanosecs but quota/period in microsecs, here we make sure we send out
        # all CPU metrics in nanosecs - that differs from linuxstats module CPU counters that are in USER_HZ.
        limit_per_sec = host_config.get('NanoCpus', 0)
        if not limit_per_sec:
            cpu_period = host_config.get('CpuPeriod', 0) or 1000000
            cpu_quota = host_config.get('CpuQuota', 0)
            if not cpu_quota:
                cpu_quota = cpu_period * len(cpu_stats)
            limit_per_sec = round(1000000000 * cpu_quota / cpu_period)
        self.buffer.append(("docker_cpu", {'limit_per_sec': limit_per_sec}, timestamp, labels.copy()))
        for k, v in enumerate(cpu_stats):
            metadata = labels.copy()
            metadata.update(name=k)
            self.buffer.append(("docker_cpu", {'usage': int(v)}, timestamp, metadata))

    def read_interface_stats(self, timestamp, labels, stats):
        keys = (
            'rx_bytes', 'rx_packets', 'rx_errors', 'rx_dropped',
            'tx_bytes', 'tx_packets', 'tx_errors', 'tx_dropped'
        )
        for k, v in stats.items():
            metadata = labels.copy()
            metadata.update(name=k)
            docker_interface_stats = {k: int(v[k]) for k in keys}
            self.buffer.append(("docker_interface", docker_interface_stats, timestamp, metadata))

    def read_memory_stats(self, timestamp, labels, stats):
        self.buffer.append(("docker_memory", {
            'used_bytes': int(stats['usage']),
            'limit_bytes': int(stats['limit']),
        }, timestamp, labels))

    def flush(self, monotonic_timestamp, system_timestamp):
        try:
            for i, container in enumerate(self.docker_client.api.containers(size=True)):
                if container.get('State') == 'running' or container.get('Status', '').startswith('Up'):
                    container_id = container['Id']
                    labels = dict(container['Labels'])
                    if 'docker_id' not in labels:
                        labels['docker_id'] = container_id[:12]
                    if 'docker_name' not in labels and container.get('Names'):
                        labels['docker_name'] = container['Names'][0]
                    inspect_info = self.docker_client.api.inspect_container(container_id)
                    host_config = inspect_info['HostConfig']
                    stats_info = self.docker_client.api.stats(container_id, decode=True, stream=False)
                    self.read_df_stats(system_timestamp, labels, int(container['SizeRootFs']), int(container.get('SizeRw', 0)))
                    self.read_cpu_stats(system_timestamp, labels, stats_info['cpu_stats']['cpu_usage'], host_config)
                    self.read_memory_stats(system_timestamp, labels, stats_info['memory_stats'])
                    self.read_interface_stats(system_timestamp, labels, stats_info['networks'])
            return super().flush(monotonic_timestamp, system_timestamp)
        except requests.exceptions.ConnectionError:
            self.log.info("Docker connection error, is docker running?")
            super().flush(monotonic_timestamp, system_timestamp)
            return False
        except ValueError:
            self.log.exception("Docker error")
            super().flush(monotonic_timestamp, system_timestamp)
            return False
