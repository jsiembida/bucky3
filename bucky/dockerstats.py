

import time
import docker
import requests.exceptions
import bucky.cfg as cfg
import bucky.common as common


class DockerStatsCollector(common.MetricsSrcProcess):
    def __init__(self, *args):
        super().__init__(*args)
        api_version = getattr(cfg, 'api_version', None)
        if api_version:
            self.docker_client = docker.client.from_env(version=api_version)
        else:
            self.docker_client = docker.client.from_env()

    def read_df_stats(self, timestamp, buf, labels, total_size, rw_size):
        docker_df_stats = {
            'total_bytes': int(total_size),
            'used_bytes': int(rw_size)
        }
        buf.append(("docker_filesystem", docker_df_stats, timestamp, labels))

    def read_cpu_stats(self, timestamp, buf, labels, stats):
        for k, v in enumerate(stats['percpu_usage']):
            metadata = labels.copy()
            metadata.update(name=k)
            buf.append(("docker_cpu", {'usage': int(v)}, timestamp, metadata))

    def read_interface_stats(self, timestamp, buf, labels, stats):
        keys = (
            'rx_bytes', 'rx_packets', 'rx_errors', 'rx_dropped',
            'tx_bytes', 'tx_packets', 'tx_errors', 'tx_dropped'
        )
        for k, v in stats.items():
            metadata = labels.copy()
            metadata.update(name=k)
            docker_interface_stats = {k: int(v[k]) for k in keys}
            buf.append(("docker_interface", docker_interface_stats, timestamp, metadata))

    def read_memory_stats(self, timestamp, buf, labels, stats):
        buf.append(("docker_memory", {'used_bytes': int(stats['usage'])}, timestamp, labels))

    def recoverable_tick(self):
        timestamp, buf = time.time(), []
        try:
            for i, container in enumerate(self.docker_client.api.containers(size=True)):
                labels = container['Labels']
                if 'docker_id' not in labels:
                    labels['docker_id'] = container['Id'][:12]
                stats = self.docker_client.api.stats(container['Id'], decode=True, stream=False)
                self.read_df_stats(timestamp, buf, labels, int(container['SizeRootFs']), int(container.get('SizeRw', 0)))
                self.read_cpu_stats(timestamp, buf, labels, stats['cpu_stats']['cpu_usage'])
                self.read_memory_stats(timestamp, buf, labels, stats['memory_stats'])
                self.read_interface_stats(timestamp, buf, labels, stats['networks'])
            if buf:
                self.send_metrics(buf)
            return True
        except (requests.exceptions.ConnectionError, ValueError):
            cfg.log.exception("Docker error")
            return False
