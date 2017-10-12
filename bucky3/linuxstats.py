

import os
import platform
import bucky3.module as module


class LinuxStatsCollector(module.MetricsSrcProcess):
    CPU_FIELDS = ('user', 'nice', 'system', 'idle', 'wait', 'interrupt', 'softirq', 'steal')
    INTERFACE_FIELDS = ('rx_bytes', 'rx_packets', 'rx_errors', 'rx_dropped',
                        None, None, None, None,
                        'tx_bytes', 'tx_packets', 'tx_errors', 'tx_dropped')
    DISK_FIELDS = ('read_ops', 'read_merged', 'read_sectors', 'read_time',
                   'write_ops', 'write_merged', 'write_sectors', 'write_time',
                   'in_progress', 'io_time', 'weighted_time')
    MEMORY_FIELDS = {
        'MemTotal:': 'total_bytes',
        'MemFree:': 'free_bytes',
        'MemAvailable:': 'available_bytes',
        'Shmem:': 'shared_bytes',
        'Cached:': 'cached_bytes',
        'Slab:': 'slab_bytes',
        'Mapped:': 'mapped_bytes',
        'SwapTotal:': 'swap_total_bytes',
        'SwapFree:': 'swap_free_bytes',
        'SwapCached:': 'swap_cached_bytes',
    }
    PROTOCOL_FIELDS = {
        'Ip:InReceives': ('ip', 'rx_packets'),
        'Ip:InDiscards': ('ip', 'rx_dropped'),
        'IpExt:InOctets': ('ip', 'rx_bytes'),
        'Ip:OutRequests': ('ip', 'tx_packets'),
        'Ip:OutDiscards': ('ip', 'tx_dropped'),
        'IpExt:OutOctets': ('ip', 'tx_bytes'),
        'Icmp:InMsgs': ('icmp', 'rx_packets'),
        'Icmp:InErrors': ('icmp', 'rx_errors'),
        'Icmp:OutMsgs': ('icmp', 'tx_packets'),
        'Icmp:OutErrors': ('icmp', 'tx_errors'),
        'Udp:InDatagrams': ('udp', 'rx_packets'),
        'Udp:InErrors': ('udp', 'rx_errors'),
        'Udp:OutDatagrams': ('udp', 'tx_packets'),
        'Udp:RcvbufErrors': ('udp', 'rcvbuf_errors'),
        'Udp:SndbufErrors': ('udp', 'sndbuf_errors'),
        'Tcp:OutSegs': ('tcp', 'tx_packets'),
        'Tcp:InSegs': ('tcp', 'rx_packets'),
        'Tcp:RetransSegs': ('tcp', 'retr_packets'),
        'Tcp:ActiveOpens': ('tcp', 'tx_opens'),
        'Tcp:PassiveOpens': ('tcp', 'rx_opens'),
        'Tcp:EstabResets': ('tcp', 'conn_resets'),
        'Tcp:CurrEstab': ('tcp', 'conn_count'),
        'Tcp:OutRsts': ('tcp', 'rx_resets'),
        'TcpExt:ListenOverflows': ('tcp', 'listen_overflows'),
        'TcpExt:ListenDrops': ('tcp', 'listen_drops'),
        'TcpExt:TCPTimeouts': ('tcp', 'timeouts'),
        'TcpExt:TCPBacklogDrop': ('tcp', 'backlog_drops'),
        'TcpExt:TCPKeepAlive': ('tcp', 'keep_alives'),
        'TcpExt:SyncookiesRecv': ('tcp', 'rx_syncookies'),
        'TcpExt:SyncookiesSent': ('tcp', 'tx_syncookies'),
    }

    def __init__(self, *args):
        assert platform.system() == 'Linux' and platform.release() >= '3'
        super().__init__(*args)

    def init_config(self):
        super().init_config()
        for name in 'interface', 'disk', 'filesystem':
            setattr(self, name + '_blacklist', self.cfg.get(name + '_blacklist', None))
            setattr(self, name + '_whitelist', self.cfg.get(name + '_whitelist', None))

    def check_lists(self, val, blacklist, whitelist):
        if whitelist:
            return val in whitelist
        if blacklist:
            return val not in blacklist
        return True

    def read_activity_stats(self, timestamp):
        activity_stats = {}
        with open('/proc/stat') as f:
            for l in f:
                tokens = l.strip().split(maxsplit=20)
                if not tokens:
                    continue
                name = tokens.pop(0)
                if not name.startswith('cpu'):
                    if name == 'ctxt':
                        activity_stats['switches'] = int(tokens[0])
                    elif name == 'processes':
                        activity_stats['forks'] = int(tokens[0])
                    elif name == 'procs_running':
                        activity_stats['running'] = int(tokens[0])
                    elif name == 'intr':
                        activity_stats['interrupts'] = int(tokens[0])
                else:
                    cpu_suffix = name[3:]
                    if not cpu_suffix:
                        continue
                    cpu_stats = {k: int(v) for k, v in zip(self.CPU_FIELDS, tokens)}
                    self.buffer.append(("system_cpu", cpu_stats, timestamp, dict(name=cpu_suffix)))
        with open('/proc/loadavg') as f:
            for l in f:
                tokens = l.strip().split()
                if tokens and len(tokens) == 5:
                    activity_stats['load'] = float(tokens[0])
                    break
        if activity_stats:
            self.buffer.append(("system_activity", activity_stats, timestamp))

    def read_filesystem_stats(self, timestamp):
        with open('/proc/mounts') as f:
            for l in f:
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
                    self.buffer.append(("system_filesystem", df_stats, timestamp,
                                        dict(device=mount_target, name=mount_path, type=mount_filesystem)))
                except OSError:
                    pass

    def read_interface_stats(self, timestamp):
        with open('/proc/net/dev') as f:
            for l in f:
                tokens = l.strip().split()
                if not tokens or len(tokens) != 17:
                    continue
                if not tokens[0].endswith(':'):
                    continue
                interface_name = tokens.pop(0)[:-1]
                if not self.check_lists(interface_name, self.interface_blacklist, self.interface_whitelist):
                    continue
                interface_stats = {k: int(v) for k, v in zip(self.INTERFACE_FIELDS, tokens) if k}
                self.buffer.append(("system_interface", interface_stats, timestamp, dict(name=interface_name)))

    def read_memory_stats(self, timestamp):
        with open('/proc/meminfo') as f:
            memory_stats = {}
            for l in f:
                tokens = l.strip().split()
                if not tokens or len(tokens) != 3 or tokens[2].lower() != 'kb':
                    continue
                name = tokens[0]
                if name in self.MEMORY_FIELDS:
                    memory_stats[self.MEMORY_FIELDS[name]] = int(tokens[1]) * 1024
            if memory_stats:
                self.buffer.append(("system_memory", memory_stats, timestamp))

    def read_disk_stats(self, timestamp):
        with open('/proc/diskstats') as f:
            for l in f:
                tokens = l.strip().split()
                if not tokens or len(tokens) != 14:
                    continue
                disk_name = tokens[2]
                if not self.check_lists(disk_name, self.disk_blacklist, self.disk_whitelist):
                    continue
                disk_stats = {k: int(v) for k, v in zip(self.DISK_FIELDS, tokens[3:])}
                disk_stats['read_bytes'] = disk_stats['read_sectors'] * 512
                disk_stats['write_bytes'] = disk_stats['write_sectors'] * 512
                self.buffer.append(("system_disk", disk_stats, timestamp, dict(name=disk_name)))

    def read_protocol_stats(self, timestamp):
        # TODO: IPv6? (/proc/net/snmp6 has a different syntax)
        param_map, proto_stats = {}, {}
        for p in '/proc/net/snmp', '/proc/net/netstat':
            with open(p) as f:
                for l in f:
                    tokens = l.strip().split()
                    if not tokens:
                        continue
                    name = tokens.pop(0)
                    if name in param_map:
                        for k, v in zip(param_map[name], tokens):
                            k = name + k
                            if k in self.PROTOCOL_FIELDS:
                                proto, value = self.PROTOCOL_FIELDS[k]
                                bucket = proto_stats.get(proto)
                                if not bucket:
                                    bucket = proto_stats[proto] = {}
                                bucket[value] = int(v)
                    else:
                        param_map[name] = tokens
        for k, v in proto_stats.items():
            self.buffer.append(("system_protocol", v, timestamp, dict(name=k)))

    def flush(self, system_timestamp):
        timestamp = system_timestamp if self.add_timestamps else None
        self.read_activity_stats(timestamp)
        self.read_memory_stats(timestamp)
        self.read_interface_stats(timestamp)
        self.read_filesystem_stats(timestamp)
        self.read_disk_stats(timestamp)
        self.read_protocol_stats(timestamp)
        return super().flush(system_timestamp)
