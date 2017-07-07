
metadata = dict(
    team="my-team",
    app="my-app",
    host="my-host"
)

sysstats = {
    "type": "system_stats",
    "log_level": "INFO",
    "interval": 1,
    "disk_blacklist": {
        "loop0", "loop1", "loop2", "loop3", "loop4", "loop5", "loop6", "loop7", "sr0"
    },
    "filesystem_blacklist": {
        "tmpfs", "devtmpfs", "rootfs"
    },
}

dockers = {
    "type": "docker_stats",
    "log_level": "INFO",
    "interval": 1
}

statsd = {
    "type": "statsd_server",
    "log_level": "INFO",
    "interval": 1,
    "local_port": 8125,
    "timers_name": "stats_timers",
    "sets_name": "stats_sets",
    "gauges_name": "stats_gauges",
    "counters_name": "stats_counters",
    "timers_timeout": 60,
    "sets_timeout": 60,
    "gauges_timeout": 300,
    "counters_timeout": 60,
    "percentile_thresholds": (90, 99)
}

carbon = {
    "type": "carbon_client",
    "log_level": "INFO",
    "interval": 1,
    "remote_hosts": [
        "127.0.0.1:2003"
    ],
    "name_mapping": (
        "team",
        "app",
        "host",
        "name",
        "value"
    ),
    "metadata": metadata
}

influxdb = {
    "type": "influxdb_client",
    "log_level": "INFO",
    "interval": 1,
    "remote_hosts": [
        "127.0.0.1:8086"
    ],
    "metadata": metadata
}

prometheus = {
    "type": "prometheus_exporter",
    "log_level": "INFO",
    "interval": 1,
    "local_port": 9090,
    "local_host": "0.0.0.0",
    "http_path": "metrics",
    "values_timeout": 600,
    "metadata": metadata
}
