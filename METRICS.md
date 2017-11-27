This documents the metrics and tags emitted by gostatsd, their type, tags, and interpretation.

| Name                                        | type    | tags            | description
| ------------------------------------------- | ------- | --------------- | -----------
| aggregator.metrics_received                 | gauge   | aggregator_id   | The number of datapoints received during the flush interval
| aggregator.processing_time                  | gauge   | aggregator_id   | The time taken (in ms) to aggregate all datapoints in this
|                                             |         |                 | flush interval
| bad_lines_seen                              | counter |                 | The number of unprocessable lines that have been seen
| events_received                             | counter |                 | The number of events received
| metrics_received                            | counter |                 | The number of metrics received
| packets_received                            | counter |                 | The number of packets received
| avg_packets_in_batch                        | gauge   |                 | The average number of packets read in a batch (up to receive-batch-size).
|                                             |         |                 | This can be used to tweak receive-batch-size if necessary to reduce memory usage
| channel.capacity                            | gauge   | channel         | The capacity of the channel
| channel.queued                              | gauge   | channel         | The absolute amount of items in a channel
| channel.pct_used                            | gauge   | channel         | The percentage of how full a channel is
| internal_dropped                            | gauge   |                 | The number of internal metrics which have been dropped in the
|                                             |         |                 | lifetime of the process.  Not a counter, because it may not be
|                                             |         |                 | submitted.
| heartbeat                                   | gauge   | version, commit | The value 0, tagged by the version (git tag) and short commit hash
| cloudprovider.aws.describeinstancecount     | gauge   |                 | The cumulative number of times DescribeInstancesPages has been called
| cloudprovider.aws.describeinstanceinstances | gauge   |                 | The cumulative number of instances which have been fed in to DescribeInstancesPages
| cloudprovider.aws.describeinstancepages     | gauge   |                 | The cumulative number of pages from DescribeInstancesPages
| cloudprovider.aws.describeinstanceerrors    | gauge   |                 | The cumulative number of errors seen from DescribeInstancesPages
| cloudprovider.aws.describeinstancefound     | gauge   |                 | The cumulative number of instances successfully found via DescribeInstances
| cloudprovider.cache_positive                | gauge   |                 | The absolute number of positive entries in the cache
| cloudprovider.cache_negative                | gauge   |                 | The absolute number of negative entries in the cache
| cloudprovider.cache_refresh_positive        | gauge   |                 | The cumulative number of positive refreshes
| cloudprovider.cache_refresh_negative        | gauge   |                 | The cumulative number of refreshes which had an error refreshing and used old data
| cloudprovider.cache_hit                     | gauge   |                 | The cumulative number of cache hits (host was in the cache)
| cloudprovider.cache_late_hit                | gauge   |                 | The cumulative number of late cache hits (host was not in the cache, but had a lookup
|                                             |         |                 | in progress which completed)
| cloudprovider.cache_miss                    | gauge   |                 | The cumulative number of cache misses
| cloudprovider.hosts_queued                  | gauge   | type            | The absolute number of hosts waiting to be looked up
| cloudprovider.items_queued                  | gauge   | type            | The absolute number of metrics or events waiting for a host lookup to complete


| Tag           | Description
| ------------- | -----------
| aggregator_id | The index of an aggregator, the amount corresponds to the --max-workers flag
| channel       | The name of an internal channel
| type          | Either metric or event


A number of channels are tracked internally, they emit metrics under the channels.* space.  They will all have a
channel tag, and may have additional tags specified below.

| Channel name        | Additional tags | Description
| ------------------- | --------------- | -----------
| dispatch_aggregator | aggregator_id   | Channel to dispatch metrics to a specific aggregator.


- If both --internal-namespace and --namespace are specified, and metrics are dispatched internally, the resulting
  metric will be namespace.internal_namespace.metric.
