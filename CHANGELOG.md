0.8.5
-----
- Remove extraneous dot in metric names for stdout and graphite backends when no tag 

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
