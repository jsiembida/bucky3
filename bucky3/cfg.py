
"""

The default configuration file for Bucky3. Also intended as a comprehensive
configuration reference. This is Python code, parsed only once during the main
module init. Once it's loaded, the main module spawns the requested modules
passing them the config in the parsed form. That way, syntactic errors in
the config make Bucky3 fail fast.

- The main module itself uses only "log_level" and "log_format" parameters. Other
parameters are module specific. All modules (including the main module) ignore
unknown params, so it is safe to define module specific params in global context.
- By design, there is no way to "live reload" or "test" the config. You have to
do the full stop/start sequence.
- Source modules: "statsd_server", "linux_stats", "docker_stats"
- Destination modules: "influxdb_client", "prometheus_exporter", "carbon_client"

"""


# log_level
# - str
# - Optional, default: 'INFO'
# - More here: https://docs.python.org/3/library/logging.html#levels
#   This option is in global context and will be used by all modules unless overridden.
log_level = "INFO"


# log_format
# - str
# - Optional, default: '[%(asctime)-15s][%(levelname)s] %(name)s(%(process)d) - %(message)s'
# - More here: https://docs.python.org/3/library/logging.html#logrecord-attributes
# - Example: log_format = '%(message)s'


# flush_interval
# - float, seconds between flush intervals
# - Required
# - In "linux_stats" and "docker_stats" it defines how often they will be taking metrics
#   and pushing metrics out to the destination module(s).
#   In "statsd_server" it defines how often the received metrics are being aggregated and
#   pushed out to the destination module(s).
#   This depends on your needs, but 10 secs is typical for the three source modules.
#   In "influxdb_client" and "carbon_client" it defines the maximum delay before the metrics
#   received from the source modules are pushed out - for these, it should stay rather low,
#   perhaps 1-5 secs.
#   In "prometheus_exporter" it defines the frequency of housekeeping wherein old metrics
#   are being found and removed. Note, it is not the maximum age of data kept in prometheus
#   module (see below for it). The check can be done at low freqs, i.e. every 60-180 secs.
#   In any case, flush_interval is enforced to be at least 0.1 sec.
flush_interval = 10


# randomize_startup
# - bool, if the module should start with a randomized delay
# - Optional, default: True
# - For modules with "flush_interval <= 3" this param is ignored, they get started asap.
#   The delay is uniformly randomized between 0...min(flush_interval, 15) - so the module
#   startup will get delayed no more than 15s regardless of the flush_interval value.
#   This setting makes most sense in source modules, most likely you want destination
#   modules start up asap.
# - Example: randomize_startup = False


# metadata
# - dict of str:str, extra metadata injected into metrics
# - Optional, default: None
# - Destination modules add this metadata to the metrics received from the source modules.
#   A union operation with metrics metadata taking precedence, keys defined here only get
#   added when non present in the metrics. Having the host name injected is very helpful,
#   other helpful could be "env", "location" or "team" - depending on your infrastructure.
metadata = dict(
    host="${BUCKY3_HOST}",
)


# buffer_limit
# - int, max number of entries in the output buffer
# - Optional, default: 10000
# - "influxdb_client" and "carbon_client" modules buffer metrics before pushing them out
#   to the destination host. This param is a safety measure. If for a reason data doesn't
#   get pushed out and the buffer grows beyond the configured buffer_limit, it is truncated
#   to buffer_limit / 2.
#   In any case, buffer_limit is enforced to be at least 100.
# - Example: buffer_limit = 1000


# chunk_size
# - int, max number of entries in one write
# - Optional, default: 300
# - In source modules it defines the max size of the batch sent out to destination modules.
#   It is a balance between too granular and too "bursty" IPC. 100 - 1000 looks reasonable.
#   In "influxdb_client" it defines the number of metrics sent out in single UDP packet,
#   if you want to keep the UDP packets within MTU, the chunk_size should be low, i.e. 5-15.
#   In "prometheus_exporter" it defines the number of metrics going into one TCP socket write,
#   the default 300 seems to be a good value here.
#   "carbon_client" doesn't use chunk_size (likely subject to change)
#   In any case, chunk_size is enforced to be at least 1.
# - Example: chunk_size = 10


# self_report
# - bool, if modules should produce metrics about themselves
# - Optional, default: False
# - Example: self_report = True


# This dictionary is a module configuration.
# The name "linuxstats" doesn't matter as such, but should be descriptive
# as it is included by default in the log formatter.
# The module inherits all options defined in the global context.
linuxstats = dict(
    # module_type
    # - str, defines the type of module to be started (see description at the top).
    # - Required
    # - There is no limit on the number of same type modules being run. This finds very
    #   limited application. I.e. you may need duplicated Prometheus exporters running
    #   on different ports. In extreme cases, you may want to run multiple statsd_server
    #   instances to make use of multiple cores.
    module_type="linux_stats",

    # module_inactive
    # - bool, de/activates the module config.
    # - Optional, default: False
    # - On non-Linux this module will fail, set the "module_inactive=True" to disable it.
    #   Or delete the whole section. It is a convenience.
    module_inactive=False,

    # disk_whitelist, disk_blacklist
    # - set of str, block devices to include/exclude
    # - Optional, default: None
    # - The matching logic is:
    #   1. If whitelist is defined and disk is in it, it is included, skip further checks
    #   2. If blacklist is defined and disk is in it, it is excluded, skip further checks
    #   3. disk is included
    #   By default, the lists are None so all disks are included. If you want to monitor
    #   only a specific set of disks, put them in a whitelist and leave the blacklist out.
    #   If you want to exclude a specific set of disks, put them in a blacklist and leave
    #   the whitelist out. Note, no regexes nor globs, just plain string matching.
    # - Example: disk_whitelist = {"sda", "sdb"}
    disk_blacklist={
        "loop0", "loop1", "loop2", "loop3",
        "loop4", "loop5", "loop6", "loop7",
        "ram0", "ram1", "ram2", "ram3",
        "ram4", "ram5", "ram6", "ram7",
        "ram8", "ram9", "ram10", "ram11",
        "ram12", "ram13", "ram14", "ram15",
        "sr0",
    },

    # filesystem_whitelist, filesystem_blacklist
    # - set of str, filesystems to include/exclude
    # - Optional, default: None
    # - See the disk_whitelist, disk_blacklist for details
    # - Example: filesystem_whitelist = {"ext4"}
    filesystem_blacklist={
        "tmpfs", "devtmpfs", "rootfs",
    },

    # interface_whitelist, interface_blacklist
    # - set of str, network interfaces to include/exclude
    # - Optional, default: None
    # - See the disk_whitelist, disk_blacklist for details
    # - Example: interface_blacklist = {"lo"}
)


# This module requires 'docker' package, i.e. `pip3 install docker` so it is disabled
# in the default config. Set "module_inactive=False" (or remove the line) to enable it.
dockerstats = dict(
    module_type="docker_stats",
    module_inactive=True,

    # api_version
    # - str, defines the docker API version to use (not the same as Docker version)
    # - Optional, default: None
    # - More here: https://docker-py.readthedocs.io/en/stable/api.html
    #   The default will auto-negotiate the protocol. You may need to specify this param
    #   when using old Docker installations, see "docker version | grep API"
    # - Example: api_version = "1.22"

    # env_mapping
    # - dict of str:str
    # - Optional, default: None
    # - Bucky3 automatically attaches some metadata to the container metrics.
    #   One of sources of the metadata can be ENV variables baked into the container, see:
    #      https://docs.docker.com/engine/reference/builder/#env
    #   This setting defines the name mappings between the ENV variable names and metadata
    #   keys injected into metrics. ENV variables not found in this mapping are ignored
    #   (so by default no ENV variables are being injected). Note, that Bucky3 also uses
    #   labels baked into the image. All the labels are unconditionally injected and take
    #   precedence over the ENV variables. See:
    #      https://docs.docker.com/engine/userguide/labels-custom-metadata/
    # Example: env_mapping={'TEAM_NAME': 'team'}
)


# This is a histogram bin constructor, it receives the metric value
# and returns a bin name (str), or None if value belongs to no bin.
# See statsd_server below for details about histogram selector.
def myapp_response_histogram(x):
    if x < 0: return None
    if x < 100: return 'under_100ms'
    if x < 300: return 'under_300ms'
    return 'over_300ms'


statsd = dict(
    module_type="statsd_server",

    # local_host
    # - str, UDP endpoint to bind at
    # - Optional, default: '0.0.0.0:0'
    # - The default will bind to a random local port, most likely you want the typical 8125.
    #   You can use IP or hostname, port number has to be numeric though.
    local_host='127.0.0.1:8125',

    # timers_bucket
    # - str, defines the metrics namespace/bucket for StatsD timers
    # - Required
    # - For the setting as below, when the statsd module receives "foo:123|ms|#hello=world"
    #   it will end up in prometheus exporter as 'stats_timers{name="foo", hello="world"} 123'
    #   The namespace/bucket can be overridden as in "foo:123|ms|#hello=world,bucket=mystuff",
    #   which will produce 'mystuff{name="foo", hello="world"} 123'
    timers_bucket="stats_timers",

    # histograms_bucket, sets_bucket, gauges_bucket, counters_bucket
    # - str, define the respective metrics namespaces/buckets
    # - Required
    # - See the timers_bucket above
    histograms_bucket="stats_histograms",
    sets_bucket="stats_sets",
    gauges_bucket="stats_gauges",
    counters_bucket="stats_counters",

    # percentile_thresholds, percentile ranges for timers
    # - tuple of float
    # - Optional, default: ()
    # - Unlike other implementations, Bucky3 doesn't do any stats unless explicitly told
    #   to do so. The setting below will give you the stats for median, 90th percentile and
    #   the totals (the 100th percentile). For each configured percentile range, the stats
    #   calculated are: lower, upper, mean, count, count_ps, and stdev.
    percentile_thresholds=(50, 90, 100),

    # histogram_selector, histogram bins for timers
    # - callable
    # - Optional, default: None
    # - When provided, this callable has to return a callable that will be used to construct
    #   histogram bins, see the myapp_response_histogram above. The selector receives
    #   the metadata dict which can be used to provide different histogram constructors
    #   depending on metrics metadata. I.e. for the packet "foo:123|h|#hello=world",
    #   the selector will receive dict(name="foo", hello="world")
    #   For each bin, the stats calculated are: lower, upper, mean, count, count_ps, and stdev.
    histogram_selector=lambda metadata: myapp_response_histogram,

    # timestamp_window, acceptable time window (in seconds) for custom timestamps
    # - int
    # - Optional, default: 600
    # - Metrics with "timestamp" metadata override the default timestamping logic which is
    #   the system "now". The custom timestamp however, is only accepted within a given time
    #   window configured here: now - timestamp_window ... now + timestamp_window
    #   Metrics with custom timestamp outside of the window are ignored.
    # - Example: timestamp_window = 60
)


# Note that InfluxDB module is disabled (it would produce UDP traffic)
influxdb = dict(
    module_type="influxdb_client",
    module_inactive=True,

    # remote_hosts, InfluxDB endpoints
    # - tuple of str
    # - Required
    # - This module fans out metrics to all configured endpoints. Also, the remote_hosts are
    #   resolved every 3min, so DNS changes are automatically picked up. Like for local_host,
    #   you can use IPs or hostnames but port number has to be numeric. If you don't specify
    #   the port, it defaults to 8086.
    # - Example: remote_hosts=("influxdb1", "influxdb2:1234")
    remote_hosts=(
        "localhost",
    ),

    # flush_interval should be short for this module so it overrides the value from
    # the global context. Also, flush_interval<=3 implies randomize_startup=False
    flush_interval=1,
    # As describe above, this module should use small chunk_size
    chunk_size=5,
)


# Note that Prometheus exporter is implicitly enabled
prometheus = dict(
    module_type="prometheus_exporter",

    # local_host, TCP endpoint to bind at
    # - str
    # - Optional, default: "0.0.0.0:9103"
    # - Example: local_host="127.0.0.1:9090"

    # http_path
    # - str
    # - Optional, default: "metrics"
    # - This module only replies to HTTP GETs for /http_path, by default it's /metrics.
    #   Other requests receive 404. If you use http_path="", the endpoint will be GET /.
    # Example: http_path=""

    # As describe above, flush_interval should be longer for this module.
    flush_interval=60,

    # values_timeout, data retention (in seconds)
    # - int
    # - Required
    # - Every flush_interval seconds this module runs a housekeeping task. The task
    #   finds all metrics that has not been refreshed (received from source modules)
    #   in values_timeout seconds and removes them. It is important that this parameter
    #   is long enough in relation to scraping frequency. Retention 2-3 x longer than
    #   scraping interval seems like a reasonable minimum.
    values_timeout=300,

    # As describe above, you likely want this module start up asap.
    randomize_startup=False,
)


# Consider the Graphite module deprecated. It is not actively maintained,
# it is provided only for backward compatibility.
carbon = dict(
    module_type="carbon_client",
    module_inactive=True,
    remote_hosts=(
        "127.0.0.1:2003",
    ),
    flush_interval=1,

    # name_mapping
    # - tuple of str
    # - Required
    # - Bucky3 speaks metrics with metadata natively. Graphite doesn't. name_mapping
    #   defines translation order from the key=value metadata to the dot separated
    #   sequence of strings of Graphite. The metrics metadata keys are pulled out in the
    #   order of name_mapping (not found keys are skipped), the keys remaining in metrics
    #   metadata afterwards are appended alphabetically. Values of thus constructed list
    #   of keys are concatenated into a dot separated Graphite string.
    #   For example, for the name_mapping as below, if the linux stats module produces:
    #   *  system_cpu(value="user", name="0") = 24
    #   After merging with global metadata it might become:
    #   *  system_cpu(value="user", name="0", host="foo", location="bar", env="test") = 24
    #   Since "bucket" is "system_cpu", Graphite receives:
    #   *  system_cpu.foo.0.user.test.bar 24
    name_mapping=(
        "bucket", "team", "app", "host", "name", "value",
    ),
)
