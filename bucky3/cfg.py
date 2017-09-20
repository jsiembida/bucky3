
log_level = "INFO"

flush_interval = 10

metadata = dict(
    host="${BUCKY3_HOST}",
)

myapp_response_histogram = (
    ('under_100ms', lambda x: x < 100),
    ('under_300ms', lambda x: x < 300),
    ('over_300ms', lambda x: True),
)

linuxstats = dict(
    # Linux specific, comment out the 'module_type' line to disable it
    module_type="linux_stats",
    disk_blacklist={
        "loop0", "loop1", "loop2", "loop3",
        "loop4", "loop5", "loop6", "loop7",
        "ram0", "ram1", "ram2", "ram3",
        "ram4", "ram5", "ram6", "ram7",
        "ram8", "ram9", "ram10", "ram11",
        "ram12", "ram13", "ram14", "ram15",
        "sr0",
    },
    filesystem_blacklist={
        "tmpfs", "devtmpfs", "rootfs",
    },
    module_inactive=True,
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
    histograms_bucket="stats_histograms",
    sets_bucket="stats_sets",
    gauges_bucket="stats_gauges",
    counters_bucket="stats_counters",
    timers_timeout=60,
    histograms_timeout=60,
    sets_timeout=60,
    gauges_timeout=300,
    counters_timeout=60,
    percentile_thresholds=(50, 90, 99, 100),
    histogram_selector=lambda key: myapp_response_histogram
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
