


### Bucky3

is a rework of [Bucky](https://github.com/trbs/bucky). Major differences include:

* Metrics with metadata are designed into Bucky3 from the ground up. This is a shift towards systems like
[InfluxDB](https://www.influxdata.com/products/influxdb/), [Prometheus](https://prometheus.io)
or [Elasticsearch](https://www.elastic.co/elasticsearch/) and the biggest conceptual difference between Bucky3
and original Bucky.
* Python 3.5+ only.
* [MetricsD](https://github.com/mojodna/metricsd) protocol has been dropped in favor
of [extended StatsD protocol.](https://docs.datadoghq.com/developers/dogstatsd/datagram_shell)
See `PROTOCOL.md` for a comprehensive description of StatsD protocol implementation in Bucky3.
* [CollectD](https://collectd.org) protocol has been dropped in favor of dedicated modules and integration
via StatsD protocol.
* Monitoring of [Docker Containers](https://www.docker.com/) is supported.
* Linux [Systemd Journal](https://www.freedesktop.org/software/systemd/man/systemd-journald.service.html)
integration is included. Bucky3 recognizes Python, Java and Node.js stacktraces in the stream of lines coming
from a system journal and stitches them back together; it also recognizes extra fields added by docker logging.

For a more complete list of differences and design choices, see `DESIGN.md`



### Quick Start

On Linux with Python3 the default configuration should start out of the box:

```
git clone https://github.com/jsiembida/bucky3.git
PYTHONPATH=bucky3 python3 -m bucky3
```

After a couple of seconds, you can harvest metrics:

```
curl -s http://127.0.0.1:9103/metrics | grep system_cpu
```

You can also feed custom metrics via StatsD protocol:

```
echo "foobar:123|g" | nc -w 1 -u 127.0.0.1 8125
```

And harvest them, too:

```
curl -s http://127.0.0.1:9103/metrics | grep foobar
```

Tests can be run as follows:

```
python3 -m unittest tests/test_*.py
```


### Slow Start

##### Configuration

There is only one, optional, command line argument. A path to configuration file. If not provided, the default
configuration is used. Please see `bucky3/cfg.py` for a detailed discussion of available configuration options.

##### Modules

Bucky3 consists of the main process, source modules and destination modules. During startup, the main process loads
configuration, dynamically imports configured modules and runs them in dedicated subprocesses. Data flow is
many-to-many, one or more source modules (subprocesses) produce metrics and send them to one or more destination
modules (subprocesses). The following modules are available:

* `statsd_server` - source module that collects metrics via extended StatsD protocol.
* `jsond_server` - source module that takes [newline delimited JSON objects](http://ndjson.org/)
via UDP protocol.
* `linux_stats` - source module that collects Linux metrics via `/proc` filesystem.
* `systemd_journal` - source module that collects logs from systemd journal.
* `docker_stats` - source module that collects metrics from running docker containers.
* `influxdb_client` - destination module that sends data to InfluxDB via
[UDP line protocol.](https://docs.influxdata.com/influxdb/v1.3/write_protocols/line_protocol_reference/)
* `prometheus_exporter` - destination module that exposes data via 
[Prometheus text exposition format.](https://prometheus.io/docs/instrumenting/exposition_formats/)
* `elasticsearch_client` - destination module that sends data to Elasticsearch via
[bulk document upload.](https://www.elastic.co/guide/en/elasticsearch/reference/5.6/docs-bulk.html)
* `debug_output` - pprints metrics, as the name suggests, intended for debugging.

##### Installation

Dedicated virtual environment is a recommended way of installing and running:

```
# python3 -m venv /usr/local/bucky3
# . /usr/local/bucky3/bin/activate
# pip install git+https://github.com/jsiembida/bucky3.git
```

##### Running

Bucky3 is intended to be run as an unprivileged service; however, docker_stats module requires access
to the docker socket and systemd_journal module requires access to journal files; both can be granted
via respective group memberships. Bucky3 doesn't store any data on filesystem, nor does it create any log files.
All logs go to stderr. Assuming the installation location as above, an example
[SystemD unit file](https://www.freedesktop.org/software/systemd/man/systemd.unit.html) could be:

```
[Unit]
Description=Bucky3, monitoring agent
After=network-online.target

[Service]
User=bucky3
Group=bucky3
SupplementaryGroups=docker
SupplementaryGroups=systemd-journal
ExecStart=/usr/local/bucky3/bin/bucky3 /etc/bucky3.conf
KillMode=control-group
TimeoutStopSec=10
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

##### Signals

When the main process receives `SIGTERM` or `SIGINT`, it sends `SIGTERM` to all modules, waits up to 5s for them
to finish and exits. The exit code in this case is zero. If any module had to be forcibly terminated with `SIGKILL`,
the exit code is non-zero. The main process ignores `SIGHUP`, use a full stop / start sequence if you need to reload it.

When a module (subprocess) receives `SIGTERM`, `SIGINT` or `SIGHUP` it flushes its buffers and exits.
When a module exits unexpectedly, i.e. outside the above shutdown sequence, the main module will restart it. 
Note that in this case, the module is not being reimported, the originally loaded module is just being restarted.
Use a full stop / start sequence if you need to reload modules from disk.



### Performance

Most of the code is I/O bound and CPU usage remains a fraction of percent. Statsd metrics should be batched
if performance is a concern. Statsd module also allows on-the-fly zlib / gzip decompression to further optimize
network traffic. Loading statsd with 1000 metrics/s, ups the CPU core utilization to around 10%.
PyPy or Cython can half it.

Tests include statsd performance measurements that are excluded by default and that can be enabled like so:

```
TEST_PERFORMANCE=yes python3 -m unittest tests/test_*.py
```
