


### StatsD Protocol

The StatsD interface on bucky3 understands most of the "DogStatsD
extended" protocol. The canonical sources for both the "vanilla" and
"extended" formats are:

- statsd: https://github.com/b/statsd_spec
- dogstatsd: https://docs.datadoghq.com/guides/dogstatsd/#datagram-format

However, since bucky3 does not support the full DogStatsD protocol, we
should consider the format in bucky3 a distinct variant. To keep the
protocol implementation focused on measurement data only, we have chosen
to ignore the "service check" and "event" message types.



### Message Format

The StatsD messages are sent as UDP datagrams with ASCII encoded
payloads. All other encodings are considered malformed data.

The protocol is line-based, and each measurement (data point)
must adhere to the following format:

    <measurement name>:<value>|<type>[|@<rate>][|#<tag name>=<tag value>[,<tag name>=<tag value>]*]<newline>

* `measurement name` is an arbitrary string
* `value` is the recorded measurement, if type is other than `c`
* `type` is one of the following:
  * `g` - gauge; an already measured value
  * `ms` - timer (milliseconds); duration of an operation
  * `h` - histogram; distribution of (often discrete) values
  * `s` - set; collection of distinct keys
  * `c` - counter; an automatically increasing count of occurrences
* `rate` is a float number between `0 < rate <= 1` that defaults to `1`
* `tag name` is an arbitrary label or tag for the value
* `tag value` is an arbitrary categorisation
* `newline` is a literal newline

For `value`, bucky3 accepts floats. Some StatsD implementations limit
`value`s to integers, but we believe that only encourages users to come
up with their own scaling factors to work around the limitation.

A message can contain zero or more tags, but usually you want to provide
at least one to assign the measurement context. The tags are commonly used
for grouping when viewing the generated time-series graphs, as well as
for specifying alert rules.

Note: unlike in DogStatsD, timer (`ms`) and histogram (`h`) are not aliases
for one another. Bucky3 implements proper histograms with customizable bins
instead of redirecting histogram messages to the timer code path.

Note: `=` is used as in `key=value` and it has the usual meaning.
DataDog documentation uses `foo:bar` but tags in DataDog implementation
are a list of strings without the meaning they have in bucky3.

Note: trailing comma after the last tag is allowed.



### Examples

Let's say you want to track the roundrip times, as well as received
and generated status codes for a login service. You measure the time
spent to make a call to another service, or maybe to read values from a
data storage.

For one particular operation the read took 4.1 milliseconds. You have
chosen to use "duration" as the measurement name for anything involving
execution time. In that case, the StatsD message payload for the
roundtrip time taken could be:

  - `duration:4.1|ms|#service=login,team=myteam,operation=read\n`

In order to track the status of their service, the team also records
each returned status code. For a service with a non-trivial amount of
activity, it is important to know how many times any particular status
code has been served. So, just before returning the service response,
the service could send the following message to bucky3:

  - `status:200|h|#service=login,team=myteam,route=/user/login\n`

Of course, if a login was unsuccesful, the message could instead be:

  - `status:401|h|#service=login,team=myteam,route=/user/login\n`

Another team maintains a service with persistent real-time connections.
In addition to response status codes, they care about roundtrip times
and the number of connected clients. Every couple of seconds, they
record the number of currently open client connections, and at a
particular moment they had 473 connections. The message payload could
then be:

  - `connections:473|g|#service=ourstream,team=otherteam\n`

In addition, every time they serve a request, they record both the
response status and the roundtrip time. So on every successful response
they could either send the following two messages:

- `status:200|h|#service=ourstream,team=otherteam,action=something\n`
- `duration:2.9|ms|#service=ourstream,team=otherteam,action=something\n`

or they could combine the values into a single message:

- `duration:2.9|ms|#service=ourstream,team=otherteam,action=something,status=200\n`

Both approaches are equally valid. Embedding a value in the message tags
makes sense when the set of possible values is small (such as HTTP codes). If the
space of possible values is large, then a separate histogram probably
makes more sense.



### The Edge Case

So far the message format has been very easy to follow, but of course
there is a special case.

Practically every service needs to record how many requests it handles
within any given time unit. The message format for counter is slightly
different because it uses a fixed value. Counter is internally
maintained by bucky3, and gets reset on every measurement window change.
A counter increase simply raises the current value by 1. A message to
increase a counter would look something like:

- `requests:1|c|#service=myservice,team=someteam,route=/some/path\n`

The counter values are most often used as sums over a time window for
measuring requests per second (RPS) or requests per minute (RPM).

Now, *technically* a counter can be increased by any amount, so the
value after the colon may be other than 1. Bucky3 does handle this
case, making it possible to nudge a counter up by arbitrary amounts.
However, in practice you should never need to do this. For sanity,
simply increase a counter always by 1.

(There are some very specific high-frequency environments, where
overriding the counter makes sense. Even then, it is recommended that
you use a gauge instead. Flooding bucky3 with extremely rapid
counter-increase messages has the potential to degrade monitoring
performance.)



### Python3 example

The following example illustrates how to send a metric to bucky3 listening
on localhost:

    import socket
    
    def send_to_bucky3_statsd(n, v, t='c', **tags):
        msg = str(n) + ':' + str(v) + '|' + str(t)
        if tags:
            msg += '|#' + ','.join(str(k) + '=' + str(v) for k, v in tags.items())
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(2)    # Not strictly necessary but a good idea
        sock.sendto(msg.encode('ascii'), ('127.0.0.1', 8125))
