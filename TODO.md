

##### High priority

* Tests for Elasticsearch client.
* More tests, especially for common modules code and the main process.
* Statsd tests for non timestamped metrics.


##### Medium priority

* IPv6 support.
* Stack tracer use a lot of regular expressions, perhaps some of that could be optimized.
  I.e. can /^\s*at / be replaced with s.lstrip().startswith('at ')?


##### Low priority

* Perhaps `SIGHUP` in the main process should be restarting all modules instead of
  being ignored? Then again, proper restart is tricky to get right. The surest shot
  is still the full stop - start cycle.
* With `timestamp` and `bucket` special keys in current statsd implementation,
  all other source modules like linux or docker stats could be implemented as statsd
  clients making the statsd module a hub. A big but tempting architectural change.
