This documents the metrics and tags emitted by gostatsd, their type, tags, and interpretation.

| Name              | type    | tags          | description
| ----------------- | ------- | ------------- | -----------
| metrics_received  | gauge   | aggregator_id | The number of datapoints received during the flush interval
| processing_time   | gauge   | aggregator_id | The time taken (in ms) to aggregate all datapoints in this
|                   |         |               | flush interval
| bad_lines_seen    | counter |               | The number of unprocessable lines that have been seen
| events_received   | counter |               | The number of events received
| metrics_received  | counter |               | The number of metrics received
| packets_received  | counter |               | The number of packets received
| channel.capacity  | gauge   | channel       | The capacity of the channel
| channel.queued    | gauge   | channel       | The absolute amount of items in a channel
| channel.pct_used  | gauge   | channel       | The percentage of how full a channel is
| internal_dropped  | gauge   |               | The number of internal metrics which have been dropped in the
|                   |         |               | lifetime of the process.  Not a counter, because it may not be
|                   |         |               | submitted.


| Tag           | Description
| ------------- | -----------
| aggregator_id | The index of an aggregator, the amount corresponds to the --max-workers flag
| channel       | The name of an internal channel


A number of channels are tracked internally, they emit metrics under the channels.* space.  They will all have a
channel tag, and may have additional tags specified below.

| Channel name        | Additional tags | Description
| ------------------- | --------------- | -----------
| dispatch_aggregator | aggregator_id   | Channel to dispatch metrics to a specific aggregator.


- The metric numStats is no longer tracked
- If both --internal-namespace and --namespace are specified, and metrics are dispatched internally, the resulting
  metric will be namespace.internal_namespace.metric.
