28.3.0
------
- Refactor the cloud handler to use the modern pipeline.  This removes the `cloudprovider.items_queued` metric with `type:metric` tag.

28.2.1
------
- Document how semver is used in the codebase.

28.2.0
------
- Changes how `parser.bad_lines_seen` and `backend.retried` metrics are sent.  They are now only sent on change.

28.1.0
------
- The Datadog backend will discard values that result in NaN or ±Inf since they can not be used with our JSON processing.

28.0.0
------
- Internal pipeline refactor removes two internal metrics

27.0.0
------
- Simplify internal tracking of the source address of a metric.  The current rules are:

If ignore-host is set, it will be set to the `host` tag if present, otherwise blank.  If ignore-host is not set, it will be set to the sending IP
If the cloud provider is enabled, it will attempt to perform a lookup of this value to find a new value (instance ID, pod ID, etc)
If the tag handler matches a `drop-host` filter, it will be removed
Backends use it inconsistently

This really is simpler than it was before.

26.0.0
------
- Adds new options `flush-offset` and `flush-aligned` to align the flush interval to a configurable multiple.  See [README.md](README.md) for details.
- Adds documentation for many of the server options.
- Internal library refactoring

25.0.0
------
- Adds InfluxDB as a backend, see [BACKENDS.md](BACKENDS.md) for details.
- Some logs have been changed to be slightly more machine readable.

24.0.1
------
- Add pprof catch-all route to match the rest of pprof's available endpoints

24.0.0
------
- Update to aggregator channel metrics. see [METRICS.md](METRICS.md) for details.

23.0.0
------
- Added experimental support for adding http headers based on metrics tags
- Fixed a bug breaking internal metrics from the backend and cloud provider


22.0.0
------
- Refactor pprof endpoints to sit under `/debug/pprof`

21.0.0
------
- Breaks out expiry interval in to per metric type expiry intervals.  See [README.md](README.md) for details.

20.3.2
------
- Use the current timestamp when sending to NewRelic backend, rather than the last updated timestamp of the metric, which may be old.

20.3.1
------
- Added a new internal metric called `backend.series.sent` which contains the number of flushed timeseries over time.

20.3.0
------
- Added experimental support for generating latency histograms for timers, more details in [README.md](README.md).

20.2.0
------
- Support for `response-header-timeout` for `transport` section

20.1.2
------
- Fixes a bug where `aggregator.metricmaps_received` are the same for for every worker

20.1.1
------
- Fixes a bug where no cloud provider would crash on startup
- Minor documentation fix

20.1.0
------
- Minor internal refactoring
- Support for regex matching in filtering, added by ermeaney

20.0.0
------
- Build with 1.13.6
- Refactor dependencies, re-run `make setup` if build failures are encountered
- Adds support for Kubernetes as a cloud provider for enriching metrics.  See [CLOUDPRIVDERS.md](CLOUDPRIVDERS.md) for details.

19.1.0
------
- Adds support for New Relics new dimensional metrics pipeline.  See [BACKENDS.md](BACKENDS.md).
- Test fixes

19.0.1
------
- Internal refactor to de-dupe 8 getSubVipers

19.0.0
------
- BREAKING: Refactor how http clients are configured.  See [TRANSPORT.md](TRANSPORT.md) for details.  Some
  configuration options are in a new location, however defaults are generally unchanged.

18.1.0
------
- Build with Go 1.13.0
- Base docker images on alpine 3.10 and ubuntu 18.04

18.0.0
------
- Support for `custom-headers` for http_forwarder_v2

17.2.0
------
- Support `log-raw-metric` configuration

17.1.2
------
- Remove support for disabling retries in the forwarder
- Document how to disable retries using timeout values

17.1.1
------
- Adding support to disable retries within the gostatsd forwarder

17.1.0
------
- NewRelic event support added by Kav91

17.0.1
------
- Moved noisy log line from info to debug

17.0.0
------
- BREAKING: Added support for [Graphite 1.1 style tags](https://grafana.com/blog/2018/01/11/graphite-1.1-teaching-an-old-dog-new-tricks/),
  details can be found in [BACKENDS.md](BACKENDS.md).
- Moved backend documentation from [README.md](README.md) to [BACKENDS.md](BACKENDS.md), still lacking documentation for most backends.

16.0.0
------
- Build with Go 1.12.3 for real.

15.0.2
------
- Fixed events in HTTP style

15.0.1
------
- Update to Alpine 3.9

15.0.0
------
- Build with Go 1.12.3
- Changed linter to golangci-lint

14.1.0
------
- Fixes internal metrics for the `dispatch_aggregator` channel stats, and `aggregator.*` metrics.

14.0.0
------
- More internal pipeline refactoring, to optimize the http ingestion path.

13.0.1
------
- Adjust log level about forwarding failures and stop logging retries.

13.0.0
------
- Removed v1 ingestion
- Refactor internal pipeline
- Added `hostname` flag to support override for internal metrics.  Defaults to system hostname.
  Use this flag when running within docker to make host identification easier.

12.0.2
------
- Fix bug with metrics going through the consolidator not having tags applied properly

12.0.1
------
- Emit v2 ingestion http metrics.

12.0.0
------
- Add metric consolidation in the HTTP path.  Minor configuration changes, specifically `metrics-per-batch` is replaced
  with `consolidator-slots`.  Defaults to the number of parsers.
- Introduces v2 ingestion.  Production use should be ok.

11.0.0
------
- Add http forwarding and ingestion.  See `Configuring the server mode` and `Configuring HTTP servers` in [README.md](README.md)
  for details.  Not recommended for production use at this time.  Healthcheck is ok to use
- Introduces v1 ingestion

10.0.1
------
- Fix linter failures

10.0.0
------
- New Relic backend supports submitting directly to Insights Collector, refer to README.md for details.

9.1.0
-----
- New Relic backend added by Kav91, see README.md for options and details

9.0.0
-----
- Added support for sampling timers to be compatible with original statsd.

8.0.0
-----
- Fixed `--statser-type` didn't apply
- Added the ability to filter tags and metrics, see FILTERING.md for details

7.2.0
-----
- New Cloudwatch backend contirubted by JorgenEvens
    - Backend name is `cloudwatch`
    - Contains the single option `namespace`, defaulting to `StatsD`
- Duplicate tags are filtered out so they are aggregated correctly.

7.1.0
-----
- Datadog user-agent has changed to `gostatsd` by default.  Can now be configured.
- New Datadog option: `user-agent`, configures the user agent supplied to Datadog.  Use `python-requests/2.6.0 CPython/2.7.10`
  for old behavior.

7.0.1
-----
- Roll back change to configuration, due to https://github.com/spf13/viper/issues/380.  Documentation is still valid.

7.0.0
-----
- Fix a bug in the cache provider where transient failures were replacing good cache
- Started passing around a logger, not used everywhere yet
- Documentation fixes

6.2.0
-----
- Added `enable-http2` flag for Datadog backend to control HTTP/2 support, defaults to `false`

6.1.2
-----
- Build with Go 1.10.2

6.1.1
-----
- Fixed a bug making the service not work on Windows.

6.1.0
-----
- Add new flag `--statser-type` to make internal metric destination configurable.  Defaults to `internal`,
  also supports `logging` and `null`

6.0.1
-----
- Fixes rate limiter on bad lines.  A value <= 0 will disable entirely.

6.0.0
-----
- Parses histogram metrics as timers
- Log bad lines with rate limit
- Add new flag `--bad-lines-per-minute`, controls the rate limit on logging lines which fail to parse.  Defaults
  to `0`.  Supports floats.

5.4.1
-----
- Memory/GC optimisation with buffers being reused in the Datadog backend
- Metrics in the cloud provider cache should now be correct

5.4.0
-----
- BREAKING: The way "per second" rates are calculated has changed from "value / configured flush interval"
  to "value / actual flush interval".

5.3.0
-----
- Uses the default EC2 credentials
- Timer sub-metrics now have a configuration option for opt-out.  See README.md for details.

5.2.2
-----
- Build -race on ubuntu:16.04 instead of 16.10

5.2.1
-----
- Performance work, round 3

5.2.0
-----
- Performance work, round 2
- Add new flag `--estimated-tags`, pre-allocates the Tags array.  Defaults to `4`.

5.1.1
-----
- Fix index out of range error and tag corruption in AWS CP

5.1.0
-----
- Performance work

5.0.0
-----
- More metrics rework.  Internal metrics are now in-phase with flushing.
- Better distribution of metrics between aggregators when received from multiple hosts.
- New Datadog option: `max_requests`, the maximum number of metric http requests that can be made by the Datadog
  backend.
- BREAKING: Additional and renamed metrics in the flusher, see METRICS.md
- BREAKING: Heartbeat changes:
    - `--heartbeat-interval` changed to `--heartbeat-enabled`.
    - Heartbeat is in-phase with the flush.
    - Heartbeat sends a value of 1, so a sum aggregation can be applied.

4.1.1
-----
- New internal metrics around the cloud provider cache and AWS cloud provider, see METRICS.md for details.

4.1.0
-----
- Add new flag `--conn-per-reader` adding support for a separate connection per
  reader (requires system support for reusing addresses)
- Refactor MetricDispatcher into BackendHandler satisfying separate MetricHandler and EventHandler interfaces
- Add TagHandler for static tags

4.0.0
-----
- BREAKING: Independent scaling of datagram reading and parsing
    - Separate MetricReceiver into DatagramReceiver and DatagramParser
    - Allow independent scaling of parser workers with new flag `--max-parsers`. Defaults to number of CPU cores.
    - Change default of `--max-readers` flag to min(8, number of CPU cores)

3.1.1
-----
- GC optimization with buffer reuse

3.1.0
-----
- New docker image suffix, `version`-syms, includes symbol table
- New flag `--receive-batch-size` to set datagram batch size.  Defaults `50`.
- Batch reading should improve performance by using `recvmmsg(2)`.  This has additional memory considerations
  documented in README.md, and can tuned by tracking the `avg_packets_in_batch` metric and adjusting as necessary.

3.0.1
-----
- New flag `--heartbeat-interval` sends heartbeat metrics on an interval, tagged by version and commit.  Defaults
  `0`.  Set to `0` to disable.

3.0.0
-----
- BREAKING: use space instead of comma to specify multiple values for the following parameters: `backends`,
  `percent-threshold`, `default-tags` and `internal-tags`.
- BREAKING: Removed Datadog `dual_stack` option in favor of explicit network selection.
- New Datadog option: `network` allows control of the network protocol used, typical values are `tcp`,
  `tcp4`, or `tcp6`.  Defaults `tcp`.  See [Dial](https://golang.org/pkg/net/#Dial) for further information.

2.4.2
-----
- New Datadog option: `dual_stack` allows control of RFC-6555 "Happy Eyeballs" for IPv6 control.  Defaults `false`.

2.4.1
-----
- No functional changes over previous version. Release tag to trigger build process.

2.4.0
-----
- Build with Go 1.9
- Add support for compression in Datadog payload.
- New Datadog option: `compress_payload` allows compression of Datadog payload.  Defaults `true`.
- Add staged shutdown
- Update logrus import path

2.3.0
-----
- New flag `--internal-tags` configures tags on internal metrics (default none)
- New flag `--internal-namespace` configures namespace on internal metrics (default "statsd")
- BREAKING: Significant internal metric changes, including new names.  See METRICS.md for details

2.2.0
-----
- New flag `--ignore-host` prevents capturing of source IP address.  Hostname can be provided
  by client via a `host:` tag.

2.1.0
-----
- Handle EC2 `InvalidInstanceID.NotFound` error gracefully
- Build with Go 1.8
- Make cloud handler cache configurable
- Batch AWS Describe Instances call
- Fix a deadlock in Cloud Handler
- Minor internals refactoring

2.0.1
-----
- Do not log an error if source instance was not found

2.0.0
-----
- BREAKING: Renamed `aws.http_timeout` into `aws.client_timeout` for consistency
- BREAKING: Renamed `datadog.timeout` into `datadog.client_timeout` for consistency
- Tweaked some timeouts on HTTP clients

1.0.0
-----
- BREAKING: Renamed parameter `maxCloudRequests` into `max-cloud-requests` for consistency
- BREAKING: Renamed parameter `burstCloudRequests` into `burst-cloud-requests` for consistency
- Fix a bunch of linting issues
- Run tests concurrently
- Configure various timeouts on HTTP clients
- Update dependencies
- Big internals refactoring and cleanup

0.15.1
------
- Fix bug in Graphite backend introduced in 0.15.0 (#75)

0.15.0
------
- Fix bug where max queue size parameter was not applied properly
- Use [context](https://golang.org/doc/go1.7#context) in more places
- Stricter TLS configuration
- Support TCP transport and write timeouts in statsd backend
- Reuse UDP/TCP sockets to reduce number of DNS lookups in statsd and graphite backends
- Reuse memory buffers in more cases
- Update dependencies

0.14.11
-------
- Go 1.7
- Config option to disable sending tags to statsdaemon backend

0.14.10
-------
- Fix NPE if cloud provider is not specified
- Minor internal cleanups

0.14.9
------
- Some additional tweaks to flushing code
- Minor refactorings

0.14.8
------
- Fix bug in rate calculation for Datadog

0.14.7
------
- Fix bug introduced in 0.14.6 in Datadog backend when invalid hostname was sent for metrics
- Set tags for own metrics (#55)
- Add [expvar](https://golang.org/pkg/expvar/) support
- Minor internals refactoring and optimization

0.14.6
------
- Limit max concurrent events (#24)
- Memory consumption optimizations

0.14.5
------
- Improved and reworked tags support; Unicode characters are preserved now.
- Minor internals refactoring and optimization

0.14.4
------
- Send start and stop events (#21)
- Linux binary is now built inside of Docker container rather than on the host

0.14.3
------
- Fix data race in Datadog backend (#44)
- Minor internals refactoring

0.14.2
------
- Fix batching support in Datadog backend

0.14.1
------
- Better Graphite support (#35)
- Minor internals refactoring

0.14.0
------
- Update to Alpine 3.4
- Cap request size in Datadog backend (#27)
- Set timeouts on Dials and tcp sends (#23)
- Reuse HTTP connections in Datadog backend

0.13.4
------
- Async rate limited cloud provider lookups (#22, #3)
- Internals refactoring

0.13.3
------
- Add configurable CPU profiler endpoint

0.13.2
------
- Increase default Datadog timeouts to reduce number of errors in the logs

0.13.1
------
- Log intermediate errors in Datadog backend
- Consistently set timeouts for AWS SDK service clients
- Update all dependencies
- Fix Datadog backend retry error #18
- Various internal improvements

0.13.0
------
- Fix goroutine start bug in dispatcher - versions 0.12.6, 0.12.7 do not work properly
- [Datadog events](http://docs.datadoghq.com/guides/overview/#events) support

0.12.7
------
- Remove deprecated -f flag passed to Docker tag command
- Rename num_stats back to numStats to be compatible with original statsd

0.12.6
------
- null backend to do benchmarking
- Internals refactoring

0.12.5
------
- Implement negative lookup cache (#8)
- Read configuration from environment, flags and config

0.12.4
------
- Do not multiply number of metric workers

0.12.3
------
- Do not replace dash and underscore in metric names and tags

0.12.2
------
- Fix handling of NaN specific case
- Minor refactorings for linter

0.12.1
------
- Use pointer to metric instead of passing by value
- Optimise (5-10x) performance of line parser

0.12.0
------
- Revert dropping message and block instead
- Use different values for number of worker to read from socket, process messages and metrics
- Minor fixes and performance improvements
- Calculate per second counters since last flush time instead of interval

0.11.2
------
- Process internal stats as standard metrics
- Add benchmarks
- Improve performance for buffering data out of the socket
- Drop messages and metrics instead of blocking when overloaded

0.11.1
------
- Normalize tags consistently but don't force lower case for metric names

0.11.0
------
- Add support for cloud plugins to retrieve host information e.g. instance id, aws tags, etc.

0.10.4
------
- Add load testing tool

0.10.3
------
- Performance improvements for receiver and aggregator

0.10.2
------
- Use goroutines to read net.PacketConn instead of buffered channel

0.10.1
------
- Graphite: replace dots by underscores in metric name (tags)
- Limit concurrency by using buffered channels for incoming messages
- Discard empty tags
- Datadog: add retries on post errors

0.10.0
------
- Add support for default tags

0.9.1
-----
- statsd backend: ensure not going over the udp datagram max size on metrics send
- Datadog: normalise tags to always be of form "key:value"

0.9.0
-----
- Reset counters, gauges, timers, etc. after an expiration delay
- Datadog: add interval to metric payload
- Datadog: use source ip address as hostname

0.8.5
-----
- Remove extraneous dot in metric names for stdout and graphite backends when no tag
- Add more and improve internal statsd stats

0.8.4
-----
- Datadog: set dogstatsd version and user-agent headers
- Datadog: use `rate` type for per second metric

0.8.3
-----
- Fix issue with tags overwriting metrics without tags

0.8.2
-----
- Fix undesired overwriting of metrics

0.8.1
-----
- Datadog: rollback change to metric names for preserving backward compatibility

0.8.0
-----
- Add more timers aggregations e.g. mean, standard deviation, etc.

0.7.1
-----
- Improve `graphite` backend reliability by re-opening connection on each metrics send
- Fix value of metrics displayed in web UI
- Improve web console UI look'n'feel

0.7.0
-----
- Add statsd backend

0.6.0
-----
- Add support for global metrics namespace
- Datadog: remove api url from config

0.5.0
-----
- Add support for "Set" metric type

0.4.2
-----
- Datadog: send number of metrics received
- Use appropriate log level for errors

0.4.1
-----
- Remove logging the url on each flush in datadog backend
- Use alpine base image for docker instead of scratch to avoid ca certs root error

0.4.0
-----
- Add datadog backend

0.3.1
-----
- Fix reset of metrics

0.3.0
-----
- Implement tags handling: use tags in metric names

0.2.0
-----
- Implement support for pluggable backends
- Add basic stdout backend
- Configure backends via toml, yaml or json configuration files
- Add support for tags and sample rate

0.1.0
-----
- Initial release
