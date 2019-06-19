Configuring backends
--------------------
Backends must be configured through the usage of a configuration file (toml, yaml and json are supported), pass via
`--config-path`.

Documentation is currently provided for `graphite` and `newrelic` backends.  For `datadog`, `statsdaemon`, `stdout`,
and `cloudwatch` please refer to the source code.

All configuration is in a stanza named after the backend, and takes simple key value pairs.

Graphite
--------
#### Example with defaults
```
[graphite]
address = "localhost:2003"
dial_timeout = '5s'
write_timeout = '30s'

mode = 'tags'

global_prefix = 'stats'
global_suffix = ''

prefix_counter = 'counters'
prefix_timer = 'timers'
prefix_gauge = 'gauges'
prefix_sets = 'sets'

```

The configuration settings are as follows:
- `address`: the graphite server to send aggregated data to
- `dial_timeout`: the timeout for connecting to the graphite server
- `write_timeout`: the maximum amount of time to try and write before giving up
- `mode`: one of `legacy`, `basic`, or `tags` style naming should be used.  Note that `legacy` and `basic` will
  silently drop all tags.  If there is a need to support tags as Graphite nodes, please raise an issue.

The following 5 options will only be applied if `mode` is `basic` or `tags`.
- `prefix_counter`: the prefix to add to all counters
- `prefix_timer`: the prefix to add to all timers
- `prefix_gauge`: the prefix to add to all gauges
- `prefix_sets`: the prefix to add to all sets
- `gloabl_prefix`: a prefix to add to all metrics

This is always applied
- `global_suffix`: a suffix to add to all metrics

#### Metric names
When `mode` is `basic` or `tags`, the graphite backend will emit metrics with the following naming scheme:

`[global_prefix.][prefix_<type>.]<metricname>[.aggregation_suffix][.global_suffix]`

The `aggregation_suffix` will be `count` or `rate` for counters, and the configured aggregation functions for timers.

When `mode` is `legacy`, the graphite backend will emit metrics with the following scheme:

- counters: `stats_counts.<metricname>[.global_suffix]` (count) and `stats.<metricname>[.global_suffix]` (rate)
- timers: `stats.timers.<metricname>.<aggregation_suffix>[.global_suffix]`
- gauges: `stats.gauges.<metricname>[.global_suffix]`
- sets: `stats.sets.<metricname>[.global_suffix]`


New Relic Backend
-----------------
Supports two routes for flushing metrics to New Relic.
- Directly to the Insights Collector - [Insights Event API](https://docs.newrelic.com/docs/insights/insights-data-sources/custom-data/send-custom-events-event-api)
- Via the Infrastructure Agent's inbuilt HTTP Server

### [New Relic Insights Event API](https://docs.newrelic.com/docs/insights/insights-data-sources/custom-data/send-custom-events-event-api)
Sending directly to the Event API alleviates the requirement of needing to have the New Relic Infrastructure Agent. Therefore you can run this from nearly anywhere for maximum flexibility. This also becomes a shorter data path with less resource requirements becoming a simpler setup.

To use this method, create an Insert API Key from here: https://insights.newrelic.com/accounts/YOUR_ACCOUNT_ID/manage/api_keys

```
#Example configuration

[newrelic]
    address = "https://insights-collector.newrelic.com/v1/accounts/YOUR_ACCOUNT_ID/events"
    api-key = "yourEventAPIInsertKey"
```

### [New Relic Infrastructure Agent](https://newrelic.com/products/infrastructure)
Sending via the Infrastructure Agent's inbuilt HTTP server provides additional features, such as automatically applying additional metadata to the event the host may have such as AWS tags, instance type, host information, labels etc.

The payload structure required to be accepted by the agent can be viewed [here.](https://github.com/newrelic/infra-integrations-sdk/blob/master/docs/v2tov3.md#v2-json-full-sample)

To enable the HTTP server, modify /etc/newrelic.yml to include the below, and restart the agent ([Step 1.2](https://docs.newrelic.com/docs/integrations/host-integrations/host-integrations-list/statsd-monitoring-integration#install)).
```
http_server_enabled: true
http_server_host: 127.0.0.1 #(default host)
http_server_port: 8001 #(default port)
```

Additional options are available to rename attributes if required.
```
[newrelic]
	tag-prefix = ""
	metric-name = "name"
	metric-type = "type"
	per-second = "per_second"
	value = "value"
	timer-min = "min"
	timer-max = "max"
	timer-count = "samples_count"
	timer-mean = "samples_mean"
	timer-median = "samples_median"
	timer-stddev = "samples_std_dev"
	timer-sum = "samples_sum"
	timer-sumsquare = "samples_sum_squares"
```
