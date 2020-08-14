gostatsd
========

[![Godoc](https://godoc.org/github.com/atlassian/gostatsd?status.svg)](https://godoc.org/github.com/atlassian/gostatsd)
[![Build Status](https://travis-ci.org/atlassian/gostatsd.svg?branch=master)](https://travis-ci.org/atlassian/gostatsd)
[![Coverage Status](https://coveralls.io/repos/github/atlassian/gostatsd/badge.svg?branch=master)](https://coveralls.io/github/atlassian/gostatsd?branch=master)
[![GitHub tag](https://img.shields.io/github/tag/atlassian/gostatsd.svg?maxAge=86400)](https://github.com/atlassian/gostatsd)
[![Docker Pulls](https://img.shields.io/docker/pulls/atlassianlabs/gostatsd.svg)](https://hub.docker.com/r/atlassianlabs/gostatsd/)
[![Docker Stars](https://img.shields.io/docker/stars/atlassianlabs/gostatsd.svg)](https://hub.docker.com/r/atlassianlabs/gostatsd/)
[![MicroBadger Layers Size](https://images.microbadger.com/badges/image/atlassianlabs/gostatsd.svg)](https://microbadger.com/images/atlassianlabs/gostatsd)
[![Go Report Card](https://goreportcard.com/badge/github.com/atlassian/gostatsd)](https://goreportcard.com/report/github.com/atlassian/gostatsd)
[![license](https://img.shields.io/github/license/atlassian/gostatsd.svg)](https://github.com/atlassian/gostatsd/blob/master/LICENSE)

An implementation of [Etsy's][etsy] [statsd][statsd] in Go,
based on original code from [@kisielk](https://github.com/kisielk/).

The project provides both a server called "gostatsd" which works much like
Etsy's version, but also provides a library for developing customized servers.

Backends are pluggable and only need to support the [backend interface](backend.go).

Being written in Go, it is able to use all cores which makes it easy to scale up the
server based on load.

Building the server
-------------------
Gostatsd currently targets Go 1.13.6.  If you are compiling from source, please ensure you are running this version.

From the `gostatsd` directory run `make build`. The binary will be built in `build/bin/<arch>/gostatsd`.

You will need to install the Golang build dependencies by running `make setup` in the `gostatsd` directory. This must be done before the first build,
and again if the dependencies change.  A [protobuf](https://github.com/protocolbuffers/protobuf) installation is expected to be found in the `tools/`
directory.  Managing this in a platform agnostic way is difficult, but PRs are welcome. Hopefully it will be sufficient to use the generated protobuf
files in the majority of cases.

If you are unable to build `gostatsd` please check your Go version, and try running `make setup` again before reporting a bug.

Running the server
------------------
`gostatsd --help` gives a complete description of available options and their
defaults. You can use `make run` to run the server with just the `stdout` backend
to display info on screen.

You can also run through `docker` by running `make run-docker` which will use `docker-compose`
to run `gostatsd` with a graphite backend and a grafana dashboard.

While not generally tested on Windows, it should work.  Maximum throughput is likely to be better on
a linux system, however.

Configuring the server mode
---------------------------
The server can currently run in two modes: `standalone` and `forwarder`.  It is configured through the top level
`server-mode` configuration setting.  The default is `standalone`.

In `standalone` mode, raw metrics are processed and aggregated as normal, and aggregated data is submitted to
configured backends (see below)

This configuration mode allows the following configuration options:
- `expiry-interval`: interval before metrics are expired, see `Metric expiry and persistence` section.  Defaults
   to `5m`.  0 to disable, -1 for immediate.
- `expiry-interval-counter`: interval before counters are expired, defaults to the value of `expiry-interval`.
- `expiry-interval-gauge`: interval before gauges are expired, defaults to the value of `expiry-interval`.
- `expiry-interval-set`: interval before sets are expired, defaults to the value of `expiry-interval`.
- `expiry-interval-timer`: interval before timers are expired, defaults to the value of `expiry-interval`.
- `flush-aligned`: whether or not the flush should be aligned.  Setting this will flush at an exact time interval.  With
  a 10 second flush-interval, if the service happens to be started at 12:47:13, then flushing will occur at 12:47:20,
  12:47:30, etc, rather than 12:47:23, 12:47:33, etc.  This removes query time ambiguity in a multi-server environment.
  Defaults to `false`.
- `flush-interval`: duration for how long to batch metrics before flushing. Should be an order of magnitude less than
  the upstream flush interval. Defaults to `1s`.
- `flush-offset`: offset for flush interval when flush alignment is enabled.  For example, with an offset of 7s and an
  interval of 10s, it will flush at 12:47:10+7 = 12:47:17, etc.
- `ignore-host`: indicates whether or not an explicit `host` field will be added to all incoming metrics and events.
  Defaults to `false`
- `max-readers`: the number of UDP receivers to run.  Defaults to 8 or the number of logical cores, whichever is less.
- `max-parsers`: the number of workers available to parse metrics.  Defaults to the number of logical cores.
- `max-workers`: the number of aggregators to process metrics.  Defaults to the number of logical cores.
- `max-queue-size`: the size of the buffers between parsers and workers.  Defaults to `10000`, monitored via
  `channel.*` metric, with `dispatch_aggregator_batch` and `dispatch_aggregator_map` channels.
- `max-concurrent-events`: the maximum number of concurrent events to be dispatching.  Defaults to `1024`, monitored
  via `channel.*` metric, with `backend_events_sem` channel.
- `estimated-tags`: provides a hint to the system as to how many tags are expected to be seen on any particular metric,
  so that memory can be pre-allocated and reducing churn.  Defaults to `4`.  Note: this is only a hint, and it is safe
  to send more.
- `log-raw-metric`: logs raw metrics received from the network.  Defaults to `false`.
- `metrics-addr`: the address to listen to metrics on. Defaults to `:8125`.
- `namespace`: a namespace to prefix all metrics with.  Defaults to ''.
- `statser-type`: configures where internal metrics are sent to.  May be `internal` which sends them to the internal
  processing pipeline, `logging` which logs them, `null` which drops them.  Defaults to `internal`, or `null` if the
  NewRelic backend is enabled.
- `percent-threshold`: configures the "percentiles" sent on timers.  Space separated string.  Defaults to `90`.
- `heartbeat-enabled`: emits a metric named `heartbeat` every flush interval, tagged by `version` and `commit`.
  Defaults to `false`.
- `receive-batch-size`: the number of datagrams to attempt to read.  It is more CPU efficient to read multiple, however
  it takes extra memory.  See [Memory allocation for read buffers] section below for details.  Defaults to 50.
- `conn-per-reader`: attempts to create a connection for every UDP receiver.  Not supported by all OS versions.
  Defaults to `false`.
- `bad-lines-per-minute`: the number of metrics which fail to parse to log per minute.  This is used to prevent a bad
  client spamming malformed statsd data, while still logging some information to enable troubleshooting.  Defaults to `0`.
- `hostname`: sets the hostname on internal metrics
- `timer-histogram-limit`: specifies the maximum number of buckets on histograms.  See [Timer histograms] below.


In `forwarder` mode, raw metrics are collected from a frontend, and instead of being aggregated they are sent via http
to another gostatsd server after passing through the processing pipeline (cloud provider, static tags, filtering, etc).

A `forwarder` server is intended to run on-host and collect metrics, forwarding them on to a central aggregation
service.  At present the central aggregation service can only scale vertically, but horizontal scaling through
clustering is planned.

Aligned flushing is deliberately not supported in `forwarder` mode, as it would impact the central aggregation server
due to all for forwarder nodes transmitting at once, and the expectation that many forwarding flushes will occur per
central flush anyway.

Configuring `forwarder` mode requires a configuration file, with a section named `http-transport`.  The raw version
spoken is not configurable per server (see [HTTP.md](HTTP.md) for version guarantees).  The configuration section allows the
following configuration options:

- `compress`: boolean indicating if the payload should be compressed.  Defaults to `true`
- `api-endpoint`: configures the endpoint to submit raw metrics to.  This setting should be just a base URL, for example
  `https://statsd-aggregator.private`, with no path.  Required, no default
- `max-requests`: maximum number of requests in flight.  Defaults to `1000` (which is probably too high)
- `max-request-elapsed-time`: duration for the maximum amount of time to try submitting data before giving up.  This
  includes retries.  Defaults to `30s` (which is probably too high). Setting this value to `-1` will disable retries.
- `consolidator-slots`: number of slots in the metric consolidator.  Memory usage is a function of this.  Lower values
  may cause blocking in the pipeline (back pressure).  A UDP only receiver will never use more than the number of
  configured parsers (`--max-parsers` option).  Defaults to the value of `--max-parsers`, but may require tuning for
  HTTP based servers.
- `transport`: see [TRANSPORT.md](TRANSPORT.md) for how to configure the transport.
- `custom-headers` : a map of strings that are added to each request sent to allow for additional network routing / request inspection.
  Not required, default is empty. Example: `--custom-headers='{"region" : "us-east-1", "service" : "event-producer"}'`
- `dynamic-headers` : similar with `custom-headers`, but the header values are extracted from metric tags matching the
  provided list of string. Tag names are canonicalized by first replacing underscores with hyphens, then converting
  first letter and each letter after a hyphen to uppercase, the rest are converted to lower case. If a tag is specified
  in both `custom-header` and `dynamic-header`, the vaule set by `custom-header` takes precedence. Not required, default
  is empty. Example: `--dynamic-headers='["region", "service"]'`.
  This is an experimental feature and it may be removed or changed in future versions.

The following settings from the previous section are also supported:
- `expiry-*`
- `ignore-host`
- `max-readers`
- `max-parsers`
- `estimated-tags`
- `log-raw-metric`
- `metrics-addr`
- `namespace`
- `statser-type`
- `heartbeat-enabled`
- `receive-batch-size`
- `conn-per-reader`
- `bad-lines-per-minute`
- `hostname`
- `log-raw-metric`


Metric expiry and persistence
-----------------------------
After a metric has been sent to the server, the server will continue to send the metric to the configured backend until
it expires, even if no additional metrics are sent from the client.  The value sent depends on the metric type:

- `counter`: sends 0 for both rate and count
- `gauge`: sends the last received value.
- `set`: sends 0
- `timer`: sends non-percentile values of 0.  Percentile values are not sent at all (see issue #135)

Setting an expiry interval of 0 will persist metrics forever.  If metrics are not carefully controlled in such an
environment, the server may run out of memory or overload the backend receiving the metrics.  Setting a negative expiry
interval will result in metrics not being persisted at all.

Each metric type has its own interval, which is configured using the following precedence (from highest to lowest):
`expiry-interval-<type>` > `expiry-interval` > default (5 minutes).


Configuring HTTP servers
------------------------
The service supports multiple HTTP servers, with different configurations for different requirements.  All http servers
are named in the top level `http-servers` setting.  It should be a space separated list of names.  Each server is then
configured by creating a section in the configuration file named `http.<servername>`.  An http server section has the
following configuration options:

- `address`: the address to bind to
- `enable-prof`: boolean indicating if profiler endpoints should be enabled. Default `false`
- `enable-expvar`: boolean indicating if expvar endpoints should be enabled. Default `false`
- `enable-ingestion`: boolean indicating if ingestion should be enabled. Default `false`
- `enable-healthcheck`: boolean indicating if healthchecks should be enabled. Default `true`

For example, to configure a server with a localhost only diagnostics endpoint, and a regular ingestion endpoint that
can sit behind an ELB, the following configuration could be used:

```config.toml
backends='stdout'
http-servers='receiver profiler'

[http.receiver]
address='0.0.0.0:8080'
enable-ingestion=true

[http.profiler]
address='127.0.0.1:6060'
enable-expvar=true
enable-prof=true
```

There is no capability to run an https server at this point in time, and no auth (which is why you might want different
addresses).  You could also put a reverse proxy in front of the service.  Documentation for the endpoints can be found
under HTTP.md

Configuring backends
--------------------
Refer to [backends](BACKENDS.md) for configuration options for the backends.

Cloud providers
--------------
Cloud providers are a way to automatically enrich metrics with metadata from a cloud vendor.

Refer to [cloud providers](CLOUDPROVIDERS.md) for configuration options for the cloud providers.


Configuring timer sub-metrics
-----------------------------
By default, timer metrics will result in aggregated metrics of the form (exact name varies by backend):
```
<base>.Count
<base>.CountPerSecond
<base>.Mean
<base>.Median
<base>.Lower
<base>.Upper
<base>.StdDev
<base>.Sum
<base>.SumSquares
```


In addition, the following aggregated metrics will be emitted for each configured percentile:
```
<base>.Count_XX
<base>.Mean_XX
<base>.Sum_XX
<base>.SumSquares_XX
<base>.Upper_XX - for positive only
<base>.Lower_-XX - for negative only
```


These can be controlled through the `disabled-sub-metrics` configuration section:
```
[disabled-sub-metrics]
# Regular metrics
count=false
count-per-second=false
mean=false
median=false
lower=false
upper=false
stddev=false
sum=false
sum-squares=false

# Percentile metrics
count-pct=false
mean-pct=false
sum-pct=false
sum-squares-pct=false
lower-pct=false
upper-pct=false
```


By default (for compatibility), they are all false and the metrics will be emitted.

Timer histograms (experimental feature)
----------------

Timer histograms inspired by [Prometheus implementation](https://prometheus.io/docs/concepts/metric_types/#histogram) can be
enabled on a per time series basis using `gsd_histogram` meta tag with value containing histogram bucketing definition (joined with `_`)
e.g. `gsd_histogram:-10_0_2.5_5_10_25_50`.

It will:
* output additional counter time series with name `<base>.histogram` and `le` tags specifying histogram buckets.
* disable default sub-aggregations for timers e.g. `<base>.Count`, `<base>.Mean`, `<base>.Upper`, `<base>.Upper_XX`, etc.

For timer with `gsd_histogram:-10_0_2.5_5_10_25_50` meta tag, following time series will be generated
* `<base>.histogram` with tag `le:-10`
* `<base>.histogram` with tag `le:0`
* `<base>.histogram` with tag `le:2.5`
* `<base>.histogram` with tag `le:5`
* `<base>.histogram` with tag `le:10`
* `<base>.histogram` with tag `le:25`
* `<base>.histogram` with tag `le:50`
* `<base>.histogram` with tag `le:+Inf`

Each time series will contain a total number of timer data points that had a value less or equal `le` value, e.g. counter `<base>.histogram` with the tag `le:5` will contain the number of all observations that had a value not bigger than `5`.
Counter `<base>.histogram` with tag `le:+Inf` is equivalent to `<base>.count` and contains the total number.

All original timer tags are preserved and added to all the time series.

To limit cardinality, `timer-histogram-limit` option can be specified to limit the number of buckets that will be created (default is `math.MaxUint32`).
Value of `0` won't disable the feature, `0` buckets will be emitted which effectively drops metrics with `gsd_hostogram` tags.

Incorrect meta tag values will be handled in best effort manner, i.e.
* `gsd_histogram:10__20_50` & `gsd_histogram:10_incorrect_20_50` will generate `le:10`, `le:20`, `le:50` and `le:+Inf` buckets
* `gsd_histogram:incorrect` will result in only `le:+Inf` bucket

This is an experimental feature and it may be removed or changed in future versions.


Load testing
------------
There is a tool under `cmd/loader` with support for a number of options which can be used to generate synthetic statsd
load.  There is also another load generation tool under `cmd/tester` which is deprecated and will be removed in a
future release.

Help for the loader tool can be found through `--help`.


Sending metrics
---------------
The server listens for UDP packets on the address given by the `--metrics-addr` flag,
aggregates them, then sends them to the backend servers given by the `--backends`
flag (space separated list of backend names).

Currently supported backends are:

* cloudwatch
* datadog
* graphite
* influxdb
* newrelic
* statsdaemon
* stdout

The format of each metric is:

    <bucket name>:<value>|<type>\n

* `<bucket name>` is a string like `abc.def.g`, just like a graphite bucket name
* `<value>` is a string representation of a floating point number
* `<type>` is one of `c`, `g`, or `ms` for "counter", "gauge", and "timer"
respectively.

A single packet can contain multiple metrics, each ending with a newline.

Optionally, `gostatsd` supports sample rates (for simple counters, and for timer counters) and tags:

* `<bucket name>:<value>|c|@<sample rate>\n` where `sample rate` is a float between 0 and 1
* `<bucket name>:<value>|c|@<sample rate>|#<tags>\n` where `tags` is a comma separated list of tags
* `<bucket name>:<value>|<type>|#<tags>\n` where `tags` is a comma separated list of tags

Tags format is: `simple` or `key:value`.


A simple way to test your installation or send metrics from a script is to use
`echo` and the [netcat][netcat] utility `nc`:

    echo 'abc.def.g:10|c' | nc -w1 -u localhost 8125

Monitoring
----------
Many metrics for the internal processes are emitted.  See METRICS.md for details.  Go expvar is also
exposed if the `--profile` flag is used.

Memory allocation for read buffers
----------------------------------
By default `gostatsd` will batch read multiple packets to optimise read performance. The amount of memory allocated
for these read buffers is determined by the config options:

    max-readers * receive-batch-size * 64KB (max packet size)

The metric `avg_packets_in_batch` can be used to track the average number of datagrams received per batch, and the
`--receive-batch-size` flag used to tune it.  There may be some benefit to tuning the `--max-readers` flag as well.

Using the library
-----------------
In your source code:

    import "github.com/atlassian/gostatsd/pkg/statsd"

Note that this project uses Go modules for dependency management.

Documentation can be found via `go doc github.com/atlassian/gostatsd/pkg/statsd` or at
https://godoc.org/github.com/atlassian/gostatsd/pkg/statsd

Versioning
----------
Gostatsd uses semver versioning for both API and configuration settings, however it does not use it for packages.

This is due to gostatsd being an application first and a library second.  Breaking API changes occur regularly, and
the overhead of managing this is too burdensome.

Contributors
------------

Pull requests, issues and comments welcome. For pull requests:

* Add tests for new features and bug fixes
* Follow the existing style
* Separate unrelated changes into multiple pull requests

See the existing issues for things to start contributing.

For bigger changes, make sure you start a discussion first by creating an issue and explaining the intended change.

Atlassian requires contributors to sign a Contributor License Agreement, known as a CLA. This serves as a record stating that the contributor is entitled to contribute the code/documentation/translation to the project and is willing to have it used in distributions and derivative works (or is willing to transfer ownership).

Prior to accepting your contributions we ask that you please follow the appropriate link below to digitally sign the CLA. The Corporate CLA is for those who are contributing as a member of an organization and the individual CLA is for those contributing as an individual.

* [CLA for corporate contributors](https://na2.docusign.net/Member/PowerFormSigning.aspx?PowerFormId=e1c17c66-ca4d-4aab-a953-2c231af4a20b)
* [CLA for individuals](https://na2.docusign.net/Member/PowerFormSigning.aspx?PowerFormId=3f94fbdc-2fbe-46ac-b14c-5d152700ae5d)

License
-------

Copyright (c) 2012 Kamil Kisiel.
Copyright @ 2016-2020 Atlassian Pty Ltd and others.

Licensed under the MIT license. See LICENSE file.

[etsy]: https://www.etsy.com
[statsd]: https://www.github.com/etsy/statsd
[netcat]: http://netcat.sourceforge.net/
