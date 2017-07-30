


### Bucky3

is a rework of [Bucky](https://github.com/trbs/bucky). Major differences include:

* Metrics with metadata are designed into Bucky3 from the ground up. This is a shift towards systems like
[InfluxDB](https://www.influxdata.com) or [Prometheus](https://prometheus.io) and biggest conceptual
difference between Bucky3 and original Bucky.
* Consequently, while support of [Carbon protocol](http://graphite.readthedocs.io/en/latest/feeding-carbon.html)
has been retained, Graphite naming style is built as a mapping on top of the underlying metadata.
* Python 3.3+ only.
* [MetricsD](https://github.com/mojodna/metricsd) protocol has been dropped in favor
of [extended StatsD protocol.](https://docs.datadoghq.com/guides/dogstatsd/#datagram-format)
* [CollectD](https://collectd.org) protocol has been dropped in favor of dedicated modules and integration via StatsD protocol.



### Quick Start

On Linux with Python3 the default configuration should start out of the box:

```
git clone https://github.com/jsiembida/bucky3.git
BUCKY3_HOST="$(hostname)" BUCKY3_TEAM="test-team" BUCKY3_APP="test-app" PYTHONPATH=bucky3 python3 bucky3/bucky3/main.py
```

After a couple of seconds, you can harvest metrics:

```
curl -s http://127.0.0.1:9103/metrics | grep system_cpu
```

You can also feed metrics via StatsD protocol:

```
echo "foobar:123|c" | nc -w 1 -u 127.0.0.1 8125
```

And harvest them, too:

```
curl -s http://127.0.0.1:9103/metrics | grep foobar
```



### Slow Start

##### Configuration

There is only one, optional, command line argument. A path to configuration file. If not provided,
the default configuration is used (see `cfg.py`). First, env variables in the file are interpolated,
then it is `exec`'ed as Python code. Python syntax therefore applies, in particular, indentation matters.
Configuration must define at least one active source module and at least one active destination module.
Module definition is a dictionary with `module_type` defined. An optional `module_inactive` can be used
to (de)activate a given module definition, it defaults to `False`.
Everything else in the configuration file is assumed to be global parameters that are inherited
by all module definitions, but they can be overridden. Example:

```
# log_level, flush_interval and metadata are global params
log_level = "INFO"
flush_interval = 10
metadata = dict(
    server_name="unnamed",
    server_location="placebury",
)

my_linux = dict(
    # A dictionary with module_type is a module configuration.
    # This module inherits log_level, flush_interval and metadata
    module_type="linux_stats",
)

my_containers = dict(
    # This is NOT an active module configuration.
    module_type="docker_stats",
    module_inactive=True,
)

my_influxdb = dict(
    # This module inherits log_level and metadata but overrides flush_interval.
    module_type="influxdb_client",
    # INFLUX_HOST env variable is used, if undefined, the configuration will fail.
    remote_hosts=( "${INFLUX_HOST}:8086", ),
    flush_interval=1,
)
```

##### Modules

Bucky3 consists of a main process, source modules and destination modules. During startup the main process loads
configuration, dynamically imports configured modules and runs them in dedicated subprocesses. Destination modules
are launched first, then source modules. Data flow is many-to-many, one or more source modules (subprocesses)
produce metrics and send them to one or more destination modules (subprocesses). There is no configuration
limit regarding a number of modules of the same type that should be launched simultaneously, although this
feature can be of use only in specific scenarios. The following modules are available:

* `module_type="statsd_server"` - source module that collects metrics via extended StatsD protocol.
* `module_type="linux_stats"` - source module that collects Linux metrics via `/proc` filesystem.
Including CPUs, system load, memory usage, filesystem usage, block devices and network interfaces activity.
* `module_type="docker_stats"` - source module that collects metrics from running docker containers. It requires
[the docker library](https://docker-py.readthedocs.io/en/stable/) but since the module is not activated by default,
the dependency is deliberately missing in `requirements.txt`. If you want to use it, you'll need to `pip install docker`
or provide it otherwise.
* `module_type="influxdb_client"` - destination module that sends metrics to InfluxDB via
[UDP line protocol.](https://docs.influxdata.com/influxdb/v1.3/write_protocols/line_protocol_reference/)
It can fan out metrics to multiple hosts and automatically detect DNS changes.
* `module_type="prometheus_exporter"` - destination module that exposes metrics via 
[Prometheus text exposition format.](https://prometheus.io/docs/instrumenting/exposition_formats/)
* `module_type="carbon_client"` - destination module that sends data to Graphite via TCP.

##### Installation

Bucky3 and its dependencies can be installed and run from a dedicated virtual environment, i.e.

```
# python3 -m venv /usr/local/bucky3
# . /usr/local/bucky3/bin/activate
# pip install git+https://github.com/jsiembida/bucky3.git
```

##### Running

Bucky3 is intended to be run as an unprivileged service / daemon. Only docker module requires that Bucky3 be
in the docker group, or otherwise have access to the docker socket. Bucky3 doesn't store any data on filesystem,
nor does it create any log files. All logs go to stdout / stderr. Assuming the installation location as above,
an example [SystemD unit file](https://www.freedesktop.org/software/systemd/man/systemd.unit.html) could be:

```
[Unit]
Description=Bucky3, monitoring agent
After=network-online.target

[Service]
User=bucky3
Group=bucky3
Environment=BUCKY3_HOST=%H
Environment=BUCKY3_TEAM=test-team
Environment=BUCKY3_APP=test-app
ExecStart=/usr/local/bucky3/bin/bucky3 /etc/bucky3.conf
KillMode=control-group
Restart=on-failure

[Install]
WantedBy=multi-user.target
Alias=bucky3.service
```

##### Signals

When the main process receives `SIGTERM` or `SIGINT`, it sends `SIGTERM` to all modules, waits for them to finish
and exits. The exit code in this case is zero. If any module had to be forcibly terminated with `SIGKILL`, the exit
code is non-zero. The main process ignores `SIGHUP`, if needed, reload should be done be stopping / restarting.

When a module (subprocess) receives `SIGTERM`, `SIGINT` or `SIGHUP` it exits. Then the main process restarts it.
Note that the module is not being reimported, the originally loaded module is just being restarted. 
