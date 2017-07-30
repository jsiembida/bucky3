
log_level = "INFO"

flush_interval = 10

metadata = dict(
    host="${BUCKY3_HOST}",
    team="${BUCKY3_TEAM}",
    app="${BUCKY3_APP}",
)

linuxstats = dict(
    # Linux specific, comment out the 'module_type' line to disable it
    module_type="linux_stats",
    disk_blacklist={
        "loop0", "loop1", "loop2", "loop3",
        "loop4", "loop5", "loop6", "loop7",
        "sr0",
    },
    filesystem_blacklist={
        "tmpfs", "devtmpfs", "rootfs",
    },
)

dockerstats = dict(
    # It depends on 'docker' package, i.e. `pip3 install docker`
    module_type="docker_stats",
    # Remove this line or set to True to activate the module
    module_inactive=True,
)

statsd = dict(
    module_type="statsd_server",
    local_port=8125,
    timers_bucket="stats_timers",
    sets_bucket="stats_sets",
    gauges_bucket="stats_gauges",
    counters_bucket="stats_counters",
    timers_timeout=60,
    sets_timeout=60,
    gauges_timeout=300,
    counters_timeout=60,
    percentile_thresholds=(90, 99),
)

carbon = dict(
    module_type="carbon_client",
    module_inactive=True,
    remote_hosts=(
        "127.0.0.1:2003",
    ),
    name_mapping=(
        "bucket", "team", "app", "host", "name", "value",
    ),
    flush_interval=1
)

influxdb = dict(
    module_type="influxdb_client",
    module_inactive=True,
    remote_hosts=(
        "127.0.0.1:8086",
    ),
    flush_interval=1
)

prometheus = dict(
    module_type="prometheus_exporter",
    local_port=9103,
    local_host="0.0.0.0",
    http_path="metrics",
    values_timeout=300,
    flush_interval=60
)
