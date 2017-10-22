


### Goal

A monitoring system capable of handling a fleet of machines / containers with sampling
frequency up to 1Hz and integrating with in-house applications. It is desired that such
a system be powerful and future proof in terms of metric processing and custom integrations.

The performance requirement can be more or less met by any well established backend,
including Graphite, InfluxDB or Prometheus. The latter however proves Graphite naming
style lacking. Hence the choice of design with more generic metadata built into it.

Infrastructures are increasingly often designed as a fleet of hosts with multiple apps
running within containers on them. In such cases, a dedicated monitoring agent running
on the host can simplify metrics collection from containers / applications and offload
monitoring backends and network.



### StatsD

is a metric protocol. Its simplicity encourages custom integrations and implementations.
But it can be powerful enough to provide needed functionality. It is push based.
In combination with a local agent it leads to a design wherein multiple applications
running on a shared host send their metrics to a known local StatsD endpoint.



### CollectD

is a popular choice for a monitoring agent. If offers a lot of integrations and has
a StatsD protocol plugin, but doesn't have the required level of metadata support in
metrics. Being a big piece of software written in C it is difficult to amend, and after
much deliberation has been deemed unfit.



### Bucky

is a multi-process monitoring agent written in Python that integrates with CollectD,
StatsD and Graphite. This is a desirable set of features. Only the "metrics with metadata"
part is missing, but Python code is easier to amend. Initially, Bucky was patched to add
metadata and InfluxDB support. This uncovered undesirable legacies in inner workings of it
i.e. untested Python3 support, "line based" IPC is Graphite-centric (suboptimal performance
in the context of IPC), naming legacies within StatsD code that proved hard to integrate
into new metadata-based scheme of things, CollectD with its naming conventions also proved
difficult to fit into the picture.



### Bucky3

is a rework of Bucky which drops the legacies and brings in metadata, InfluxDB and
Prometheus support as first-class citizens. It also drops a couple of features for
the sake of simplicity. Below is a non-exhaustive list of differences between Bucky
and Bucky3 as well highlights of some of Bucky3 features:

* Python3 is the target platform. 
* Bucky has a dedicated subprocess for custom metrics processor, Bucky3 drops that
and uses a configurable hook for filtering. Also, backends like InfluxDB / Prometheus
offer rich data processing and custom data postprocessing is most likely redundant.
* CollectD protocol has been dropped. Linux metrics are implemented in ~200 lines
of Python code to provide replacement for some of the lost CollectD functionality.
* Docker containers' metrics collection got implemented in a similar amount of code.
* Bucky uses `setproctitle` to set human friendly names for its processes. This is a nice
feature, but the dependency is problematic, i.e. `pip install setproctitle` needs to
compile native code. It's been therefore removed in favor of simplicity.
* MetricsD protocol has been dropped, it overlaps with StatsD protocol.
* Inspired by DataDog agent, StatsD protocol has been extended to support metadata.
The goal is to provide an integration mechanism that is simple yet powerful.
* StatsD protocol has been further extended to properly handle histograms (typically,
StatsD implementations just alias histograms to timers, which is not quite correct).
* The StatsD implementation in Bucky3 also allows the client to override `timestamp`
and `bucket` of the metrics.
* Bucky can be configured partially with command line options and a configuration file.
This duality is gone, Bucky3 only takes a configuration file. Configuration syntax is
cleaned up and adds environment variables interpolation.
* Bucky persists StatsD gauges, Bucky3 doesn't (it stores no data by design). Bucky3 has
a different but uniform way of evicting old data.
* Options related to UID, GID and nice levels are gone. Service managers like `runit`,
`daemontools`, `systemd`, etc provide those and much more.
* The legacy naming style in StatsD is gone, no effort is made to stay compatible with
the old StatsD in favor of clean design with metadata.
* Bucky has an option for debugging Graphite plaintext protocol, it is dropped as
protocol debugging can be done with tools like `netcat` or `tcpdump`.
* Bucky has an option to use pickle protocol with Graphite (and it uses batch transfers
for it as opposed to the plaintext protocol) - Bucky3 drops the pickle protocol, retains
the plaintext protocol and uses batch transfers across the board.
* Bucky uses multiprocess architecture with threading in StatsD, Bucky3 retains the
multiprocess architecture and avoids threading by design. StatsD got single threaded.
The only exception is Prometheus exporter with HTTP server running in a dedicated thread.
* The main process in Bucky passes messages from sources to Graphite module. The main
process in Bucky3 takes no part in IPC, all IPC stays between source and destination
modules.
* IPC in Bucky is "line-based", one datapoint - one message. Bucky3 uses batches
everywhere, more efficient and conceptually closer to its data model.
* Bucky imports modules at source level, Bucky3 does it dynamically. This is to avoid
dependencies in the main process. Moreover, to improve isolation, Bucky3 delays modules
initialization (i.e. socket / file operations) until after fork.
* Codebase has got reduced from `~2600 loc` to `~1400 loc`
