
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
- Source modules:
    statsd_server, jsond_server, linux_stats, docker_stats
- Destination modules:
    influxdb_client, prometheus_exporter, carbon_client, elasticsearch_client

"""


# log_level
# - str
# - Optional, default: 'INFO'
# - More here: https://docs.python.org/3/library/logging.html#levels
#   This option is in global context and will be used by all modules unless overridden.
log_level = "INFO"


# log_format
# - str
# - Optional, default: '%(asctime)s %(levelname)s %(name)s %(threadName)s@%(process)d %(message)s'
# - More here: https://docs.python.org/3/library/logging.html#logrecord-attributes
#   Note that the "ps" command will show identical command lines for all modules, to work
#   around it, the default log format includes PID.
# - Example: log_format = '%(message)s'


# flush_interval
# - float, seconds between flush intervals
# - Required
# - In linux_stats and docker_stats it defines how often they will be taking metrics
#   and pushing them out to the destination module(s).
#   In statsd_server it defines how often the received metrics are being aggregated and
#   pushed out to the destination module(s). Similarly, in jsond_server, although it does
#   not do any aggregation.
#   This depends on your needs, but 10 secs is typical for the source modules.
#   In influxdb_client, carbon_client and elasticsearch_client it defines the maximum
#   delay before the metrics received from the source modules are pushed out - for these,
#   it should stay rather low, perhaps 1-5 secs.
#   In prometheus_exporter it defines the frequency of housekeeping wherein old metrics
#   are being evicted from cache. Note, it is not the maximum age of data kept in prometheus
#   module, see also add_timestamps option and prometheus exporter section below for more.
#   In any case, flush_interval is enforced to be at least 1 sec.
flush_interval = 10


# max_flush_interval
# - float, limit on flush interval in case of failures (seconds)
# - Optional, default: 600
# - If module fails flushing its buffers, the flush_interval is being doubled on each
#   failure (exponentially growing interval). This parameter limits this. If it's smaller
#   than flush_interval, flush_interval is used as the limit, which means, there is no back off.
# - Example: max_flush_interval = 60


# socket_timeout
# - float, timeout for socket operations (when applicable), in seconds
# - Optional, default: None
# - If None, no timeouts are being set on sockets (which most likely defaults to some runtime
#   specific value). If set, it will be enforced to be at least 1 sec.
# - Example: socket_timeout = 10


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
# - Optional, default: {}
# - Source modules merge this metadata into the produced metrics. Having the host name
#   injected is very helpful, other helpful could be "env", "location" or "team".
metadata = {
    'env': 'default-config',
}


# buffer_limit
# - int, max number of entries in the output buffer
# - Optional, default: 10000
# - influxdb_client, carbon_client and elasticsearch_client modules buffer metrics
#   before pushing them out to the destination host. This param is a safety measure. If for
#   a reason data doesn't get pushed out and the buffer grows beyond buffer_limit, it is
#   truncated to buffer_limit / 2.
#   In any case, buffer_limit is enforced to be at least 100.
# - Example: buffer_limit = 1000


# chunk_size
# - int, max number of entries in one write
# - Optional, default: 300
# - In source modules it defines the max size of the batch sent out to destination modules.
#   It is a balance between too granular and too "bursty" IPC. 100 - 1000 looks reasonable.
#   In influxdb_client it defines the number of metrics sent out in single UDP packet,
#   if you want to keep the UDP packets within MTU, the chunk_size should be low, i.e. 5-15.
#   In carbon_client and prometheus_exporter it defines the number of metrics going into
#   one TCP socket write, the default 300 seems to be a good value here.
#   In elasticsearch_client it defines a number of entries in one bulk upload. Bigger bulk
#   calls are significantly more efficient in Elasticsearch. The default 300 or bigger is fine.
#   In any case, chunk_size is enforced to be at least 1.
# - Example: chunk_size = 10


# self_report
# - bool, if modules should produce metrics about themselves
# - Optional, default: False
# - Each source and each destination module will report CPU usage, MEM usage and uptime
#   when this option is on. Self reporting is throttled to roughly max(flush_interval, 60).
#   So the self report will be produced no more often than once a minute, but may be produced
#   less often if the flush period is long. Note, the main module, by design, will not report
#   anything regardless of the setting. Also, the configured metadata is injected into self
#   reported metrics just as it would for other type of metrics.
# - Example: self_report = True


# add_timestamps
# - bool, if metrics produced should have timestamps added
# - Optional, default: False
# - Each source module can add timestamps to the metrics they produce.
#   Prometheus2 comes with new metrics timeout semantics - if you want to use it, leave
#   this option off.
#   For InfluxDB and Prometheus that option doesn't matter much so long as the flush_window
#   is relatively short. If unsure, switch it on.
#   For Carbon and Elasticsearch, you want this option being on (but the modules will work
#   regardless)
#   In any case, metrics coming via StatsD protocol with explicitly provided timestamps
#   will always have timestamps included. This is to provide capability of backfilling
#   late metrics and for those new Prometheus 2 semantics won't work anyway.
# - Example: add_timestamps = True


# push_time_limit
# - float, time limit in seconds for a single flush
# - Optional, default: = flush_interval / 2
# - It can happen that sending the data out takes too long. This parameter sets a total time
#   limit a single flush operation can take. This is only required for influxdb_client,
#   carbon_client and elasticsearch_client and should be reasonably shorter than flush_interval.
# - Example: push_time_limit = 0.3


# push_count_limit
# - int, max number or records pushed in a single flush
# - Optional, default: = buffer_limit
# - Similarly to push_time_limit, this parameter sets a boundary on how much data we flush
#   in one go. Also only applicable to influxdb_client, carbon_client and elasticsearch_client.
#   push_count_limit, buffer_limit and chunk_size define the buffering policy for those three
#   modules and provide control over traffic generated and data retention.


# metric_postprocessor
# - callback, custom metric postprocessor
# - Optional, default: None
# - Each source module can run metrics through extra postprocessing right before pushing
#   them out to destination module(s). The postprocessor can return an altered metric or
#   None, in the latter case the metric is dropped. Postprocessor receives the metric with
#   custom metadata injected. The example below would drop all metrics with env=test.
# - Example: metric_postprocessor = ignore_test_environment

def ignore_test_environment(bucket, values, timestamp, metadata):
    if metadata.get('env') == 'test':
        return None
    return bucket, values, timestamp, metadata


# This dictionary is a module configuration.
# The name "linuxstats" doesn't matter as such, but should be descriptive
# as it is included by default in the log formatter.
# The module inherits all options defined in the global context.
linuxstats = {
    # module_type
    # - str, defines the type of module to be started (see description at the top).
    # - Required
    # - There is no limit on the number of same type modules being run. This finds very
    #   limited application. I.e. you may need duplicated Prometheus exporters running
    #   on different ports. In extreme cases, you may want to run multiple statsd_server
    #   instances to make use of multiple cores.
    'module_type': "linux_stats",

    # module_inactive
    # - bool, de/activates the module config.
    # - Optional, default: False
    # - On non-Linux this module will fail, set the "module_inactive=True" to disable it.
    #   Or delete the whole section. This option is a convenience.
    'module_inactive': False,

    # destination_modules
    # - list / tuple of names / references
    # - Optional, default: all
    # - This specifies which destination(s) will be receiving metrics from this source.
    #   These are either string names of the dictionaries or code level references
    #   (in that case, order of definitions matters). By default, metrics are fanned out
    #   to all destination modules configured.
    # - Example: 'destination_modules': ('prometheus', influxdb),

    # disk_whitelist, disk_blacklist
    # - set of str, block devices to include/exclude
    # - Optional, default: None
    # - The matching logic is:
    #   1. If whitelist is defined and disk matches it, it is included, skip further checks
    #   2. If blacklist is defined and disk matches it, it is excluded, skip further checks
    #   3. disk is included
    #   By default, the lists are None so all disks are included. If you want to monitor
    #   only a specific set of disks, put them in a whitelist and leave the blacklist out.
    #   If you want to exclude a specific set of disks, put them in a blacklist and leave
    #   the whitelist out. The strings are used as fully anchored regular expressions.
    # - Example: 'disk_whitelist': {"sd[a-z]", "xvd.+"},
    'disk_blacklist': {
        r"loop\d+", r"ram\d+", r"sr\d+",
    },

    # filesystem_whitelist, filesystem_blacklist
    # - set of str, filesystems to include/exclude
    # - Optional, default: None
    # - See the disk_whitelist, disk_blacklist for details
    # - Example: 'filesystem_whitelist': {"ext[234]"},
    'filesystem_blacklist': {
        "tmpfs", "devtmpfs", "rootfs", "squashfs",
    },

    # interface_whitelist, interface_blacklist
    # - set of str, network interfaces to include/exclude
    # - Optional, default: None
    # - See the disk_whitelist, disk_blacklist for details
    # - Example: 'interface_blacklist': {"lo", "veth.+"},
}


# This module only works on linux as it uses /proc & /sys to collect containers' metrics.
# Set "module_inactive=False" (or remove the line) to enable it.
dockerstats = {
    'module_type': "docker_stats",
    'module_inactive': True,

    # api_version
    # - str, defines the docker API version to use (not the same as Docker version)
    # - Optional, default: '1.22'
    # - More here: https://docs.docker.com/engine/api/v1.32/#section/Versioning
    #   Note that a newer docker daemon will likely handle older API calls, but the reverse
    #   will fail. See "docker version | grep API".
    # - Example: 'api_version': "1.22",

    # docker_socket
    # - str, unix socket for docker
    # - Optional, default: '/var/run/docker.sock'
    # - Note that this module can only use local unix sockets for docker API calls.
    # - Example: 'docker_socket': '/var/run/docker.sock',

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
    # - Example: 'env_mapping': {'TEAM_NAME': 'team'},
}


# This is a histogram bin constructor, it receives the metric value
# and returns a bin name (str), or None if value belongs to no bin.
# See statsd_server below for details about histogram selector.
def myapp_response_histogram(x):
    if x < 0: return None
    if x < 100: return 'under_100ms'
    if x < 300: return 'under_300ms'
    return 'over_300ms'


statsd = {
    'module_type': "statsd_server",

    # local_host
    # - str, UDP endpoint to bind at
    # - Optional, default: '0.0.0.0:0'
    # - The default will bind to a random local port, most likely you want the typical 8125.
    #   You can use IP or hostname, port number has to be numeric though.
    'local_host': '127.0.0.1:8125',

    # timers_bucket
    # - str, defines the metrics namespace/bucket for StatsD timers
    # - Required
    # - For the setting as below, when the statsd module receives "foo:123|ms|#hello=world"
    #   it will end up in prometheus exporter as 'stats_timers{name="foo", hello="world"} 123'
    #   The namespace/bucket can be overridden as in "foo:123|ms|#hello=world,bucket=mystuff",
    #   which will produce 'mystuff{name="foo", hello="world"} 123'
    'timers_bucket': "stats_timers",

    # histograms_bucket, sets_bucket, gauges_bucket, counters_bucket
    # - str, define the respective metrics namespaces/buckets
    # - Required
    # - See the timers_bucket above
    'histograms_bucket': "stats_histograms",
    'sets_bucket': "stats_sets",
    'gauges_bucket': "stats_gauges",
    'counters_bucket': "stats_counters",

    # percentile_thresholds, percentile ranges for timers
    # - tuple of float
    # - Optional, default: ()
    # - Unlike other implementations, Bucky3 doesn't do any stats unless explicitly told
    #   to do so. The setting below will give you the stats for median, 90th percentile and
    #   the totals (the 100th percentile). For each configured percentile range, the stats
    #   calculated are: lower, upper, mean, count, count_ps, and stdev.
    'percentile_thresholds': (50, 90, 100),

    # histogram_selector, histogram bins for timers
    # - callable
    # - Optional, default: None
    # - When provided, this callable has to return a callable that will be used to construct
    #   histogram bins, see the myapp_response_histogram above. The selector receives
    #   the metadata dict which can be used to provide different histogram constructors
    #   depending on metrics metadata. I.e. for the packet "foo:123|h|#hello=world",
    #   the selector will receive {'name': "foo", 'hello': "world"}
    #   For each bin, the stats calculated are: lower, upper, mean, count, count_ps, and stdev.
    'histogram_selector': lambda metadata: myapp_response_histogram,

    # timestamp_window, acceptable time window (in seconds) for custom timestamps
    # - int
    # - Optional, default: 600
    # - Metrics with "timestamp" metadata override the default timestamping logic which is
    #   the system "now". The custom timestamp however, is only accepted within a given time
    #   window configured here: now - timestamp_window ... now + timestamp_window
    #   Metrics with custom timestamp outside of the window are ignored.
    # - Example: 'timestamp_window': 60,
}


# This module consumes via UDP protocol newline delimited JSON objects (and only objects)
# The JSON objects as such obviate the need of having metadata aside of metric(s) values,
# the metadata configured is still merged into the objects as for other source modules.
# There is no aggregation carried out on the JSON objects, they are only buffered and
# flushed to the destination modules at the configured flush_interval.
# There are limits as to what is going to be accepted. Only an object and only string,
# number, boolean and null values in it - this implies no nested structures.
# Note there is no protection against malicious payloads, it is as secure as Python's
# built-in json module.
jsond = {
    'module_type': "jsond_server",
    'module_inactive': True,

    # local_host
    # - str, UDP endpoint to bind at
    # - Optional, default: '0.0.0.0:0'
    # - See local_host description statsd, you most likely want to specify this option.
    'local_host': '127.0.0.1:8181',

    # timestamp_window, acceptable time window (in seconds) for custom timestamps
    # - int
    # - Optional, default: 600
    # - See timestamp_window in statsd module.
    # - Example: 'timestamp_window': 60,
}


# This is linux specific module. It reads systemd journal events. See:
# https://www.freedesktop.org/software/systemd/python-systemd/journal.html
systemd = {
    'module_type': "systemd_journal",
    'module_inactive': True,

    # journal_log_level, event severity level threshold
    # - str
    # - Optional, default: 'INFO'
    # - This parameter is similar to log_level. It however applies to events coming from
    #   the system journal. Note that you provide Python's log levels here, they are being
    #   mapped by the module to syslog levels (because that's what journal events use).
    #   Journal events coming without log level are getting this level assigned by default,
    #   and then get processed, too.
    # - Example: 'journal_log_level': 'ERROR',

    # trace_log_level, severity/priority level for reassembled traces
    # - str
    # - Optional, default: None
    # - If not None, this parameter defines the level of stack traces reassembled from
    #   journal line stream. If None, traces have level coming from the first line.
    # - Example: 'trace_log_level': 'ERROR',

    # level_field_name, custom field name
    # - str
    # - Optional, default: 'level'
    # - This module always injects level/severity/priority to the produced events
    #   (see journal_log_level above). This parameter defines the name under which it is being injected.
    # - Example: 'level_field_name': 'severity',

    # facility_field_name, custom field name
    # - str
    # - Optional, default: 'facility'
    # - This module can inject facility to the produced events. This parameter defines the name
    #   under which it is being injected, if set to None, facility is not being injected.
    #   under is defined here.
    # - Example: 'level_field_name': None,

    # timestamp_window, time window (in seconds) for past events
    # - int
    # - Optional, default: 60
    # - You may want to receive events from before Bucky3 was started (i.e. boot messages)
    #   This parameter defines how far back should the module read the events from journal.
    #   Set it to zero, to disable fetching the past events.
    # - Example: 'timestamp_window': 180,

    # decode_json
    # - bool
    # - Optional, default: False
    # - If True, the module will try to parse messages received from journal as json payload,
    #   and if succeeds in doing so, the result will be flattened. Same caveats as in jsond_server.
    # - Example: 'decode_json': True,

    # journal_bucket
    # - str
    # - Optional, default: 'logs'
    # - This parameter allows to customize the 'bucket' metadata attached to the data produced
    #   by this module.
    # - Example: 'journal_bucket': 'my-systemd-logs'
}


# Note that InfluxDB module is disabled (it would produce UDP traffic)
influxdb = {
    'module_type': "influxdb_client",
    'module_inactive': True,

    # remote_hosts, InfluxDB endpoints
    # - tuple of str
    # - Required
    # - This module fans out metrics to all configured endpoints. Also, the remote_hosts are
    #   resolved every 3min, so DNS changes are automatically picked up. Like for local_host,
    #   you can use IPs or hostnames but port number has to be numeric. If you don't specify
    #   the port, it defaults to 8086.
    # - Example: 'remote_hosts': ("influxdb1", "influxdb2:1234"),
    'remote_hosts': (
        "localhost",
    ),

    # flush_interval should be short for this module so it overrides the value from
    # the global context. Also, flush_interval<=3 implies randomize_startup=False
    'flush_interval': 1,
    # As describe above, this module should use small chunk_size
    'chunk_size': 5,
}


# This is an example of a dynamic ES index name generator.
# It should return a str or None, in the latter case, the metric will be dropped.
# See index_name option in the elasticsearch module below.
def elasticsearch_index_generator(bucket, values, timestamp):
    from datetime import datetime
    return datetime.utcfromtimestamp(timestamp).strftime('metrics_%Y_%m_%d')


elasticsearch = {
    'module_type': "elasticsearch_client",
    'module_inactive': True,

    # index_name, Elasticsearch index name
    # - str or callable
    # - Optional, default: None
    # - If not provided, the destination index names are bucket names, see also:
    #   https://www.elastic.co/blog/index-type-parent-child-join-now-future-in-elasticsearch
    #   https://www.elastic.co/guide/en/elasticsearch/guide/current/mapping.html
    #   Note, that this can be a function, see elasticsearch_index_generator above.
    #   If the function returns None instead of str, the datapoint is dropped.
    # - Example: 'index_name': "graylog_deflector",

    # add_type
    # - bool
    # - Optional, default: False
    # - If True, _type field will be added as defined by type_name.
    #   Note, that ES7 drops supports for _type and ES6 deprecates it. You should be using
    #   this option only when you are using ES5 or older, see:
    #   https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html
    # - Example: 'add_type': True,

    # type_name, Elasticsearch type name, see also add_type
    # - str
    # - Optional, default: None
    # - If not provided, the destination type names are bucket names, see also index_name.
    #   Note, if add_type is False, this option is ignored and type name is never added.
    # - Example: 'type_name': "message",

    # bucket_field_name
    # - str
    # - Optional, default: 'bucket'
    # - If not None, an extra field with the datapoint bucket name is added. The name of the field
    #   is defined by this parameter. It is intended as a replacement for _type (see add_type and type_name).
    #   You should only be using this option when you are using ES6 or newer. In ES5 it duplicates the function
    #   of the required _type field anyway, so you most likely should set this option to None.
    # - Example: 'bucket_field_name': None,

    # timestamp_field_name, custom field name
    # - str
    # - Optional, default: 'timestamp'
    # - If not None, this module will add a field with millis since epoch. The name of the field
    #   is defined by this parameter. You want some form of timestamping in your Elasticsearch
    #   documents, but you can provide it in your docs and switch timestamping in this module off.
    #   Note, the timestamp produced by this option is a number of millis from epoch, in ES configuration
    #   known as epoch_millis. See: https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
    # - Example: 'timestamp_field_name': '@timestamp',

    # remote_hosts, Elasticsearch endpoints
    # - tuple of str
    # - Required
    # - See remote_hosts in InfluxDB module. Elasticsearch endpoints are TCP and this
    #   client sends data to a randomly picked one from the pool of resolved endpoints.
    #   The connection is recycled every 3min. This is to provide load balancing that
    #   follows topology changes. The default port is 9200.
    # - Example: 'remote_hosts': ("es1", "es2:1234"),
    'remote_hosts': (
        "localhost",
    ),

    'flush_interval': 1,

    # compression, whether to compress HTTP payload in Elasticsearch API calls
    # - str
    # - Optional, default: None
    # - In heavy load setups, compressing JSON bulk uploads can save a lot of bandwidth.
    #   Acceptable values are: 'deflate' and 'gzip'
    # - Example: 'compression': 'deflate',
}


# Note that Prometheus exporter is implicitly enabled
prometheus = {
    'module_type': "prometheus_exporter",

    # local_host, TCP endpoint to bind at
    # - str
    # - Optional, default: "0.0.0.0:9103"
    # - Example: 'local_host': "127.0.0.1:9090",

    # http_path
    # - str
    # - Optional, default: "metrics"
    # - This module only replies to GETs for /http_path, by default it is GET /metrics
    #   Other requests receive 404. If you use http_path="", the endpoint will be GET /
    # - Example: 'http_path': "",

    # flush_interval in combination with values_timeout define the metrics retention
    # in prometheus exporter. Prometheus1 has a long staleness (configured to a point)
    # for which it makes sense to have flush_interval and values_timeout long.
    # Prometheus2 introduces short metric retention, if you want to take advantage of it
    # you need to tune flush_interval and values_timeout to your scraping interval.
    # Here, "flush_interval=5" and "values_timeout=14" are set so that statsd module
    # drops metrics before 20s - which will work with 10s scraping interval.
    'flush_interval': 5,

    # values_timeout, data retention (in seconds)
    # - int
    # - Required
    # - Every flush_interval seconds this module runs a housekeeping task. The task
    #   finds all metrics that has not been refreshed (received from source modules)
    #   in values_timeout seconds and removes them.
    'values_timeout': 14,

    # As described above, you likely want this module start up asap.
    'randomize_startup': False,

    # compression, whether to compress HTTP response
    # - str
    # - Optional, default: None
    # - Note that unlike Elasticsearch, Prometheus can only accept 'gzip' encoding.
    #   Also, the module will only use gzip when client offers it with 'Accept-Encoding'.
    'compression': 'gzip',
}


# Consider the Graphite module deprecated. It is not actively maintained,
# it is provided only for backward compatibility.
carbon = {
    'module_type': "carbon_client",
    'module_inactive': True,
    'remote_hosts': (
        "127.0.0.1:2003",
    ),
    'flush_interval': 1,

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
    'name_mapping': (
        "bucket", "team", "app", "host", "name", "value",
    ),
}
