


### Data models

This document describes data structures internally produced by Bucky3.
Subject to merging to configured metadata and translation in the destination module.



#### Linux stats module

##### system_cpu (/proc/stat)

Payload:

    {
        'idle': int,                   All fields are integer counters
        'interrupt': int,
        'nice': int,
        'softirq': int,
        'steal': int,
        'system': int,
        'user': int,
        'wait': int,
    }

Metadata:

    {
        'name': str,                   I.e. 'cpu0', 'cpu1'...
    }

##### system_activity (/proc/stat, /proc/loadavg)

Payload:

    {
        'forks': int counter,
        'interrupts': int counter,
        'load': float gauge,
        'running': int gauge,
        'switches': int counter,
    }

##### system_memory (/proc/meminfo)

Payload:

    {
        'available_bytes': int,        All fields are integer gauges
        'cached_bytes': int,
        'free_bytes': int,
        'mapped_bytes': int,
        'shared_bytes': int,
        'slab_bytes': int,
        'swap_cached_bytes': int,
        'swap_free_bytes': int,
        'swap_total_bytes': int,
        'total_bytes': int,
    }

##### system_interface (/proc/net/dev)

Payload:

    {
        'rx_bytes': int,               All fields are integer counters
        'rx_dropped': int,
        'rx_errors': int,
        'rx_packets': int,
        'tx_bytes': int,
        'tx_dropped': int,
        'tx_errors': int,
        'tx_packets': int,
    }

Metadata:

    {
        'name': str,                   I.e. 'lo', 'eth0'...
    }
