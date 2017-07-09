
log_level = "DEBUG"

flush_interval = 1

metadata = dict(
    team="my-team",
    app="my-app",
    host="my-host",
)

sysstats = dict(
    module_type="system_stats",
    disk_blacklist={
        "loop0", "loop1", "loop2", "loop3",
        "loop4", "loop5", "loop6", "loop7",
        "sr0",
    },
    filesystem_blacklist={
        "tmpfs", "devtmpfs", "rootfs",
    },
)

dockers = dict(
    # module_type="docker_stats",
)

statsd = dict(
    # module_type="statsd_server",
    local_port=8125,
    timers_name="stats_timers",
    sets_name="stats_sets",
    gauges_name="stats_gauges",
    counters_name="stats_counters",
    timers_timeout=60,
    sets_timeout=60,
    gauges_timeout=300,
    counters_timeout=60,
    percentile_thresholds=(50, 90, 99),
)

carbon = dict(
    module_type="carbon_client",
    remote_hosts=(
        "127.0.0.1:2003",
    ),
    name_mapping=(
        "bucket", "team", "app", "host", "name", "value",
    ),
)

influxdb = dict(
    module_type="influxdb_client",
    remote_hosts=(
        "127.0.0.1:8086",
    ),
)

prometheus = dict(
    module_type="prometheus_exporter",
    local_port=9090,
    local_host="0.0.0.0",
    http_path="metrics",
    values_timeout=60,
)
