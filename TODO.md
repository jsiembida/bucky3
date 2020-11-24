

##### High priority

* Tests for Elasticsearch client.
* Establish Elasticsearch friendly naming convention, currently metrics names
  coming from different sources might clash leading to incorrect type autodetection.
  More specific names could alleviate it. Suffixes could also indicate types
  of metrics and the facilitate type templates. I.e. current:
     system_cpu name=1 user=123 nice=99 idle=10
  could be:
     system_cpu cpu_name=1 user_tick_count=123 nice_tick_count=99 idle_tick_count=10
  This will however break compatibility so perhaps should be allowed as a configurable
  option.
* More tests, especially for common modules code and the main process.
* Statsd tests for non timestamped metrics.


##### Medium priority

* IPv6 support.


##### Low priority

* With `timestamp` and `bucket` special keys in current statsd implementation,
  all other source modules like linux or docker stats could be implemented as statsd
  clients making the statsd module a hub. A big but tempting architectural change.
