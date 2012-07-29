/* 

Package statsd implements functionality for creating servers compatible with the statsd protocol.
See https://github.com/b/statsd_spec for a description of the protocol.

The main components of the library are MetricReceiver and MetricAggregator,
which are responsible for receiving and aggregating the metrics respectively.
MetricAggregator receives Metric objects via its MetricChan and then aggregates
them based on their type. At every FlushInterval the metrics are flushed via
the aggregator's associated MetricSender object.

Currently the library implements just one type MetricSender, compatible with Graphite
(http://graphite.wikidot.org), but any object implementing the MetricSender
interface can be used with the library.

*/
package statsd
