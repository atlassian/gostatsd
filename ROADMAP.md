Roadmap
-------

Pri 1
-----

* [x] Support `Set` metric
* [x] Improve `graphite` backend reliability by re-opening connection on each flush or implementing retry on error
* [x] Add `statsd` backend to allow load balancing the statsd server / running statsd server HA
* [x] Add `datadog` backend
* [x] Add more timers aggregations e.g. mean, standard deviation, etc.
* [ ] Add tests
* [x] Add load testing
* [x] Add benchmarks
* [x] Review reset of counters, gauges, timers, etc. using last flush time and an expiration window
* [x] Add support for global metrics namespace
* [x] Add support for default tags
* [x] Improve internal statsd stats

Pri 2
-----

* [ ] Add support for gauge deltas and timestamp lag
* [x] Add interval for datadog backend
* [x] Add retries on datadog backend
* [x] Use source ip address as hostname for datadog backend
* [x] Use datadog and user agent headers for datadog backend
* [ ] Add `kinesis` backend
* [ ] Fix gomatelinter issues
* [ ] Implement stats by backend e.g. last flush, last flush error, etc.
* [ ] Support `Event` metric
* [ ] Fix value of metrics displayed in console
* [x] Fix value of metrics displayed in web UI
* [ ] Rate limiting per source ip address
* [x] Ensure not going over the udp datagram max size when sending to statsd backend

Pri 3
-----

* [ ] Add `influxdb` backend
* [ ] Support `Histogram` metric
* [x] Improve web console UI look'n'feel
* [x] Add support for cloud plugins to retrieve host information e.g. instance id, aws tags, etc.
* [x] Calculate per second counters since last flush time instead of interval
