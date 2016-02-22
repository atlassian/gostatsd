/*

Package statsd implements functionality for creating servers compatible with the statsd protocol.
See https://github.com/etsy/statsd/blob/master/docs/metric_types.md for a description of the protocol.

The main components of the library are MetricReceiver and MetricAggregator,
which are responsible for receiving and aggregating the metrics respectively.
MetricAggregator receives Metric objects via its MetricChan and then aggregates
them based on their type. At every FlushInterval the metrics are flushed via
the aggregator's associated backend MetricSender objects.

Currently the library implements just a few types of MetricSender, one compatible with Graphite
(http://graphite.wikidot.org), one for Datadog and one just for stdout, but any object implementing the MetricSender
interface can be used with the library. See available backends at
https://github.com/jtblin/gostatsd/tree/master/backend/backends.

As with the orginal etsy statsd, multiple backends can be used simultaneously.
*/
package statsd
