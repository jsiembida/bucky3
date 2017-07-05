

import os
import time
import bucky.cfg as cfg
import bucky.common as common


class SystemStatsCollector(common.MetricsSrcProcess):
    # The order of cpu fields in /proc/stat
    CPU_FIELDS = ('user', 'nice', 'system', 'idle', 'wait', 'interrupt', 'softirq', 'steal')

    def get_lists(self, name):
        blacklist = getattr(cfg, name + '_blacklist', None)
        whitelist = getattr(cfg, name + '_whitelist', None)
        return blacklist, whitelist

    def check_lists(self, val, blacklist, whitelist):
        if whitelist:
            return val in whitelist
        if blacklist:
            return val not in blacklist
        return True

    def read_cpu_stats(self, timestamp, buf):
        with open('/proc/stat') as f:
            processes_stats = {}
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens:
                    continue
                name = tokens[0]
                if not name.startswith('cpu'):
                    if name == 'ctxt':
                        processes_stats['switches'] = int(tokens[1])
                    elif name == 'processes':
                        processes_stats['forks'] = int(tokens[1])
                    elif name == 'procs_running':
                        processes_stats['running'] = int(tokens[1])
                else:
                    cpu_suffix = name[3:]
                    if not cpu_suffix:
                        continue
                    cpu_stats = {k: int(v) for k, v in zip(self.CPU_FIELDS, tokens[1:])}
                    buf.append(("system_cpu", cpu_stats, timestamp, dict(name=cpu_suffix)))
            if processes_stats:
                buf.append(("system_processes", processes_stats, timestamp))

    def read_filesystem_stats(self, timestamp, buf):
        with open('/proc/mounts') as f:
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens or len(tokens) != 6:
                    continue
                if not tokens[1].startswith('/'):
                    continue
                mount_target, mount_path, mount_filesystem = tokens[:3]
                if not self.check_lists(mount_filesystem, self.filesystem_blacklist, self.filesystem_whitelist):
                    continue
                try:
                    stats = os.statvfs(mount_path)
                    total_inodes = int(stats.f_files)
                    # Skip special filesystems
                    if not total_inodes:
                        continue
                    block_size = stats.f_bsize
                    df_stats = {
                        'free_bytes': int(stats.f_bavail) * block_size,
                        'total_bytes': int(stats.f_blocks) * block_size,
                        'free_inodes': int(stats.f_favail),
                        'total_inodes': total_inodes
                    }
                    buf.append(("system_filesystem", df_stats, timestamp, dict(device=mount_target, name=mount_path, type=mount_filesystem)))
                except OSError:
                    pass

    def read_interface_stats(self, timestamp, buf):
        with open('/proc/net/dev') as f:
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens or len(tokens) != 17:
                    continue
                if not tokens[0].endswith(':'):
                    continue
                interface_name = tokens[0][:-1]
                if not self.check_lists(interface_name, self.interface_blacklist, self.interface_whitelist):
                    continue
                interface_stats = {
                    'rx_bytes': int(tokens[1]),
                    'rx_packets': int(tokens[2]),
                    'rx_errors': int(tokens[3]),
                    'rx_dropped': int(tokens[4]),
                    'tx_bytes': int(tokens[9]),
                    'tx_packets': int(tokens[10]),
                    'tx_errors': int(tokens[11]),
                    'tx_dropped': int(tokens[12])
                }
                buf.append(("system_interface", interface_stats, timestamp, dict(name=interface_name)))

    def read_load_stats(self, timestamp, buf):
        with open('/proc/loadavg') as f:
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens or len(tokens) != 5:
                    continue
                load_stats = {
                    'last_1m': float(tokens[0]),
                    'last_5m': float(tokens[1]),
                    'last_15m': float(tokens[2])
                }
                buf.append(("system_load", load_stats, timestamp))

    def read_memory_stats(self, timestamp, buf):
        with open('/proc/meminfo') as f:
            memory_stats = {}
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens or len(tokens) != 3 or tokens[2].lower() != 'kb':
                    continue
                name = tokens[0]
                if not name.endswith(":"):
                    continue
                name = name[:-1].lower()
                if name == "memtotal":
                    memory_stats['total_bytes'] = int(tokens[1]) * 1024
                elif name == "memfree":
                    memory_stats['free_bytes'] = int(tokens[1]) * 1024
                elif name == "memavailable":
                    memory_stats['available_bytes'] = int(tokens[1]) * 1024
            if memory_stats:
                buf.append(("system_memory", memory_stats, timestamp))

    def read_disk_stats(self, timestamp, buf):
        with open('/proc/diskstats') as f:
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens or len(tokens) != 14:
                    continue
                disk_name = tokens[2]
                if not self.check_lists(disk_name, self.disk_blacklist, self.disk_whitelist):
                    continue
                disk_stats = {
                    'read_ops': int(tokens[3]),
                    'read_merged': int(tokens[4]),
                    'read_sectors': int(tokens[5]),
                    'read_bytes': int(tokens[5]) * 512,
                    'read_time': int(tokens[6]),

                    'write_ops': int(tokens[7]),
                    'write_merged': int(tokens[8]),
                    'write_sectors': int(tokens[9]),
                    'write_bytes': int(tokens[9]) * 512,
                    'write_time': int(tokens[10]),

                    'in_progress': int(tokens[11]),
                    'io_time': int(tokens[12]),
                    'weighted_time': int(tokens[13])
                }
                buf.append(("system_disk", disk_stats, timestamp, dict(name=disk_name)))

    def tick(self):
        # TODO refactor it
        self.filesystem_blacklist, self.filesystem_whitelist = self.get_lists('filesystem')
        self.interface_blacklist, self.interface_whitelist = self.get_lists('interface')
        self.disk_blacklist, self.disk_whitelist = self.get_lists('disk')

        timestamp, buf = int(time.time()), []
        self.read_cpu_stats(timestamp, buf)
        self.read_load_stats(timestamp, buf)
        self.read_memory_stats(timestamp, buf)
        self.read_interface_stats(timestamp, buf)
        self.read_filesystem_stats(timestamp, buf)
        self.read_disk_stats(timestamp, buf)
        self.send_metrics(buf)
        return True
