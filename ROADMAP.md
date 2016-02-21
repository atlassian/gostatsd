Roadmap
-------

Pri 1
-----

* [ ] Support `Set` metric
* [ ] Improve `graphite` backend reliability by re-opening connection on each flush or implementing retry on error
* [ ] Add `statsd` backend to allow load balancing the statsd server / running statsd server HA
* [x] Add `datadog` backend
* [ ] Add `influxdb` backend
* [ ] Add more aggregations e.g. mean, standard deviation, etc.
* [ ] Add tests
* [ ] Add load testing
* [ ] Review reset of counters, gauges, timers, etc. using last flush time and an expiration window

Pri 2
-----

* [ ] Fix gomatelinter issues
* [ ] Implement stats by backend e.g. last flush, last flush error, etc.
* [ ] Support `Histogram` metric
* [ ] Support `Event` metric

Pri 3
-----

* [ ] Improve web console UI

