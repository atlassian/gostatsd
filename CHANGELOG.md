0.6.0
-----
- Add support for global metrics namespace

0.5.0
-----
- Add support for "Set" metric type

0.4.2
-----
- datadog: send number of metrics received
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
