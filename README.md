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
server based on load. The server can also be run HA and be scaled out, see
[Load balancing and scaling out](https://github.com/atlassian/gostatsd#load-balancing-and-scaling-out).

Building the server
-------------------
Gostatsd currently targets Go 1.12.3.  If you are compiling from source, please ensure you are running this version.

From the `gostatsd` directory run `make build`. The binary will be built in `build/bin/<arch>/gostatsd`.

You will need to install the Golang build dependencies by running `make setup` in the `gostatsd` directory. This must be done before the first build,
and again if the dependencies change.  A [protobuf](https://github.com/protocolbuffers/protobuf) installation is expected to be found in the `tools/`
directory.  Managing this in a platform agnostic way is difficult, but PRs are welcome. Hopefully it will be sufficient to use the generated protobuf
files in the majority of cases.

If you are unable to build `gostatsd` please check your go version, and try running `make setup` again before reporting a bug.

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

In `forwarder` mode, raw metrics are collected from a frontend, and instead of being aggregated they are sent via http
to another gostatsd server after passing through the processing pipeline (cloud provider, static tags, filtering, etc).

A `forwarder` server is intended to run on-host and collect metrics, forwarding them on to a central aggregation
service.  At present the central aggregation service can only scale vertically, but horizontal scaling through
clustering is planned.

Configuring `forwarder` mode requires a configuration file, with a section named `http-transport`.  The raw version
spoken is not configurable per server (see [HTTP.md] for version guarantees).  The configuration section allows the
following configuration options:

- `client-timeout`: duration for the http client timeout.  Defaults to `10s`
- `compress`: boolean indicating if the payload should be compressed.  Defaults to `true`
- `enable-http2`: boolean to enable the usage of http2 on the request.  There seems to be some incompatibility with the
  golang http2 implementation and AWS ELB/ALBs.  If you experience strange timeouts and hangs, this should be the first
  thing to disable.  Defaults to `false`
- `api-endpoint`: configures the endpoint to submit raw metrics to.  This setting should be just a base URL, for example
  `https://statsd-aggregator.private`, with no path.  Required, no default
- `max-requests`: maximum number of requests in flight.  Defaults to `1000` (which is probably too high)
- `max-request-elapsed-time`: duration for the maximum amount of time to try submitting data before giving up.  This
  includes retries.  Defaults to `30s` (which is probably too high)
- `network`: the network type to use, probably `tcp`, `tcp4`, or `tcp6`.  Defaults to `tcp`
- `consolidator-slots`: number of slots in the metric consolidator.  Memory usage is a function of this.  Lower values
  may cause blocking in the pipeline (back pressure).  A UDP only receiver will never use more than the number of
  configured parsers (`--max-parsers` option).  Defaults to the value of `--max-parsers`, but may require tuning for
  HTTP based servers.
- `flush-interval`: duration for how long to batch metrics before flushing. Should be an order of magnitude less than
  the upstream flush interval. Defaults to `1s`

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
Refer to [BACKENDS.md] for configuration options for the backends

Cloud providers
--------------
Cloud providers are a way to automatically enrich metrics with metadata from a cloud vendor.  Currently only AWS is
supported.  If enabled, the AWS cloudprovider will set the `host` to the instance id, collect all the EC2 tags, and
the region.

They should be disabled on the aggregation server when using http forwarding, as the source IP isn't propagated, and
that information should be collected on the ingestion server.


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

Timer histograms inspired by [Promehteus implementaion](https://prometheus.io/docs/concepts/metric_types/#histogram) can be
enabled on a per time series basis using `gsd_histogram` meta tag with value containing histogram bucketing definition (joined with `_`) 
e.g. `gsd_histogram:-10_0_2.5_5_10_25_50`.

It will:
* output additional cumulative counter time series with name `<base>.histogram` and `le` tags specifying histogram buckets.
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

Each team series will contain total number of timer data points that had value less or equal `le` value, e.g. cumulative counter `<base>.histogram` with tag `le:5` will contain number of all observations that had value below `5`.
Cumulative counter `<base>.histogram` with tag `le:+Inf` is equivalent to `<base>.count` and contains total number.

All other timer tags are preserved and added to all the time series.

To limit cardinality, `timer-histogram-limit` option can be specified to limit number of buckets that will be created (default is `math.MaxUint32`).

This is experimental feature and it is possible that it will be removed in future versions.

Sending metrics
---------------
The server listens for UDP packets on the address given by the `--metrics-addr` flag,
aggregates them, then sends them to the backend servers given by the `--backends`
flag (space separated list of backend names).

Currently supported backends are:

* graphite
* datadog
* statsdaemon
* stdout
* cloudwatch
* newrelic

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

Documentation can be found via `go doc github.com/atlassian/gostatsd/pkg/statsd` or at
https://godoc.org/github.com/atlassian/gostatsd/pkg/statsd

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
Copyright @ 2016-2017 Atlassian Pty Ltd and others.

Licensed under the MIT license. See LICENSE file.

[etsy]: https://www.etsy.com
[statsd]: https://www.github.com/etsy/statsd
[netcat]: http://netcat.sourceforge.net/
