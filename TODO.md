

##### High priority

* More tests, especially for common modules code and the main process.
* Provide a default configuration file with ***ALL*** configurable parameters detailed
  in it. This file should serve as a comprehensive, up to date, and only configuration
  reference.
* Document StatsD protocol implementation. In detail sufficient for somebody to write
  a fully featured client without resorting to RTFS.


##### Medium priority

* Linux stats module could collect some metrics for network protocols
  (see `/proc/net/snmp` and `/proc/net/netstat`)
* Parametrize some of the hardcoded constants, i.e.
    - StatsD module has hardcoded time window of 600s for acceptable custom timestamps
    - Prometheus module has hardcoded chunk size of 300 for writes to TCP socket
    - Similarly, InfluxDB has hardcoded chunk size of 5 for UDP line protocol
    - Hostname resolution is refreshed in hardcoded intervals
    - Main process has hardcoded failure ratio for a module to be considered dead
* Sending massive TCP payloads proved problematic in Prometheus exporter, so it does
  a sequence of smaller writes to the socket instead. Graphite module still does one
  massive write to the socket and most likely may need to do the smaller writes, too.


##### Low priority

* Docker stats module uses `limit_per_sec`, this is cosmetic but inconsistent
  with the statsd module that uses a similar `count_ps`
* Perhaps `SIGHUP` in the main process should be restarting all modules instead of
  being ignored? Then again, proper restart is tricky to get right. The surest shot
  is still the full stop - start cycle.
* Graphite module uses one randomly selected host from the resolved list as the actual
  destination for metrics, InfluxDB module uses all hosts to fan out metrics to.
  Graphite module should ideally use all resolved hosts, too.
* Some stats in Statsd module seem to be either pointless or redundant, i.e.
    - Median of timers in a bucket is always calculated explicitly, even though
      it can be configured as one of percentile thresholds
    - Mean and stddev can be calculated by the backend from sums and counts
* Bucky3 could expose some metrics about itself.
* With `timestamp` and `bucket` special keys in current statsd implementation,
  all other source modules like linux or docker stats could be implemented as statsd
  clients making the statsd module as a hub. A big but tempting architectural change.
