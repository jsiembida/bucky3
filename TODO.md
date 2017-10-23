

##### High priority

* More tests, especially for common modules code and the main process.
* Statsd tests for non timestamped metrics 


##### Medium priority
 
* Sending massive TCP payloads proved problematic in Prometheus exporter, so it does
  a sequence of smaller writes to the socket instead. Graphite module still does one
  massive write to the socket and most likely may need to do the smaller writes, too.


##### Low priority

* Perhaps `SIGHUP` in the main process should be restarting all modules instead of
  being ignored? Then again, proper restart is tricky to get right. The surest shot
  is still the full stop - start cycle.
* Graphite module uses one randomly selected host from the resolved list as the actual
  destination for metrics, InfluxDB module uses all hosts to fan out metrics to.
  Graphite module should ideally use all resolved hosts, too.
* With `timestamp` and `bucket` special keys in current statsd implementation,
  all other source modules like linux or docker stats could be implemented as statsd
  clients making the statsd module a hub. A big but tempting architectural change.
