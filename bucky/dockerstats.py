

import time
import docker
import requests.exceptions
import bucky.common as common


class DockerStatsCollector(common.MetricsSrcProcess):
    def __init__(self, *args):
        super().__init__(*args)

    def loop(self):
        api_version = self.cfg.get('api_version', None)
        if api_version:
            self.docker_client = docker.client.from_env(version=api_version)
        else:
            self.docker_client = docker.client.from_env()
        super().loop()

    def read_df_stats(self, timestamp, labels, total_size, rw_size):
        docker_df_stats = {
            'total_bytes': int(total_size),
            'used_bytes': int(rw_size)
        }
        self.buffer.append(("docker_filesystem", docker_df_stats, timestamp, labels))

    def read_cpu_stats(self, timestamp, labels, stats):
        for k, v in enumerate(stats['percpu_usage']):
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
        self.buffer.append(("docker_memory", {'used_bytes': int(stats['usage'])}, timestamp, labels))

    def flush(self):
        timestamp = round(time.time(), 3)
        try:
            for i, container in enumerate(self.docker_client.api.containers(size=True)):
                labels = container['Labels']
                if 'docker_id' not in labels:
                    labels['docker_id'] = container['Id'][:12]
                stats = self.docker_client.api.stats(container['Id'], decode=True, stream=False)
                self.read_df_stats(timestamp, labels, int(container['SizeRootFs']), int(container.get('SizeRw', 0)))
                self.read_cpu_stats(timestamp, labels, stats['cpu_stats']['cpu_usage'])
                self.read_memory_stats(timestamp, labels, stats['memory_stats'])
                self.read_interface_stats(timestamp, labels, stats['networks'])
            return super().flush()
        except (requests.exceptions.ConnectionError, ValueError):
            self.log.exception("Docker error")
            super().flush()
            return False
