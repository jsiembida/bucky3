


### Bucky3

is a rework of [Bucky](https://github.com/trbs/bucky). Major differences include:

* Metrics with metadata are designed into Bucky3 from the ground up. This is a shift towards systems like
[InfluxDB](https://www.influxdata.com) or [Prometheus](https://prometheus.io) and biggest conceptual
difference between Bucky3 and original Bucky.
* Consequently, while support of [Carbon protocol](http://graphite.readthedocs.io/en/latest/feeding-carbon.html)
has been retained, Graphite naming style is built as a mapping on top of the underlying metadata.
* Python 3.4+ only.
* [MetricsD](https://github.com/mojodna/metricsd) protocol has been dropped in favor
of [extended StatsD protocol.](https://docs.datadoghq.com/guides/dogstatsd/#datagram-format).
See `PROTOCOL.md` for a comprehensive description of StatsD protocol implementation in Bucky3.
* [CollectD](https://collectd.org) protocol has been dropped in favor of dedicated modules and integration
via StatsD protocol.

For a more complete list of differences and design choices, see `DESIGN.md`


### Quick Start

On Linux with Python3 the default configuration should start out of the box:

```
git clone https://github.com/jsiembida/bucky3.git
BUCKY3_HOST="$(hostname)" PYTHONPATH=bucky3 python3 bucky3/bucky3/main.py
```

After a couple of seconds, you can harvest metrics:

```
curl -s http://127.0.0.1:9103/metrics | grep system_cpu
```

You can also feed metrics via StatsD protocol:

```
echo "foobar:123|g" | nc -w 1 -u 127.0.0.1 8125
```

And harvest them, too:

```
curl -s http://127.0.0.1:9103/metrics | grep foobar
```

From within the project directory, you can run tests:

```
python3 -m unittest tests/test_*.py
```



### Slow Start

##### Configuration

There is only one, optional, command line argument. A path to configuration file. If not provided, the default
configuration is used. Please see `bucky3/cfg.py` for a detailed discussion of Bucky3 configuration.

##### Modules

Bucky3 consists of the main process, source modules and destination modules. During startup the main process loads
configuration, dynamically imports configured modules and runs them in dedicated subprocesses. Data flow is
many-to-many, one or more source modules (subprocesses) produce metrics and send them to one or more destination
modules (subprocesses). The following modules are available:

* `statsd_server` - source module that collects metrics via extended StatsD protocol.
* `linux_stats` - source module that collects Linux metrics via `/proc` filesystem.
Including CPUs, system load, memory usage, filesystem usage, block devices and network interfaces activity.
* `docker_stats` - source module that collects metrics from running docker containers. It requires
[the docker library](https://docker-py.readthedocs.io/en/stable/) but since the module is not activated by default,
the dependency is deliberately missing in `requirements.txt`. If you want to use it, you'll need to `pip install docker`
or provide it otherwise.
* `influxdb_client` - destination module that sends metrics to InfluxDB via
[UDP line protocol.](https://docs.influxdata.com/influxdb/v1.3/write_protocols/line_protocol_reference/)
It can fan out metrics to multiple hosts and automatically detect DNS changes.
* `prometheus_exporter` - destination module that exposes metrics via 
[Prometheus text exposition format.](https://prometheus.io/docs/instrumenting/exposition_formats/)
* `carbon_client` - destination module that sends data to Graphite via TCP.

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
code is non-zero. The main process ignores `SIGHUP`, use a full stop / start sequence if you need to reload it.

When a module (subprocess) receives `SIGTERM`, `SIGINT` or `SIGHUP` it exits. Then the main process restarts it.
Note that the module is not being reimported, the originally loaded module is just being restarted. Use a full
stop / start sequence if you need to reload modules from disk.
