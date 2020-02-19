# Filtering
Filtering is a way of cleaning up data in the pipeline, as it may be quicker to add a filter while waiting for upstream
data to be cleaned up.  All filtering is done before aggregation, as the timeseries may conflict if tags are stripped
post-aggregation.  As such, it is more expensive to evaluate, and CPU usage should be monitored carefully.  This feature
should be used as a temporary band-aid only.

## Configuration
Filtering requires a configuration file at present, and can't be specified on the command line.  It starts with the
`filters` key, which is a list of filter names, either TOML style or space separated.  Each filter is then defined in
its own block, named `filter.<filter name>`.

## The filter block
A filter block contains up to 6 keys.  3 for filtering rules, and 3 for actions to take if the rules match.

| Name            | Meaning
| --------------- | -------
| match-metrics   | A list of matches to apply to the metric name.  If the metric name doesn't match anything in this list, it is excluded from further filtering.  If the list is empty, it is not evaluated and the metric may be filtered.
| exclude-metrics | A list of matches to apply to the metric name.  If the metric name matches anything in this list, it is excluded from further filtering.
| match-tags      | A list of matches to apply to the metrics tags.  If any tag in the metric matches any tag in the list, the metric will be filtered.
| drop-tags       | A list of tags which will be stripped off the metric if the filter matches.
| drop-metric     | The entire metric will be dropped if the filter matches.
| drop-host       | The hostname will be stripped off the metric if the filter matches.

## Matching
A match is defined as a case sensitive string with an optional ! prefix to invert the meaning, and an optional * suffix
to indicate it is a prefix match.  Note: it is not a wildcard, it is a prefix match only.
## Regex matching
If a match is prefixed with `regex:` (after the `!` if you want it inverted) then the rest of the pattern is a golang regex. The trailing `*` behavior is diffrent as it is part of the regex and not a prefix match.

Examples:
- abc - matches the "abc", but not ABC or abcd
- abc* - matches "abc" and "abcd"
- !abc - matches "xyz" and "abcd" but not "abc"
- !abc* - matches "xyz" but not "abc" or "abcd"
- regex:.*abc.* - matches "xyz.abc.123" but not "xyz.123"
- !regex:.*abc.* - matches "xyz.123" but not "xyz.abc.123"

## Filter examples

Drops the host tag and hostname of any metric named as global.*
```
filters='make-global noisy-tag drop-subset'

[filter.make-global]
match-metrics='global.*'
drop-host=true
drop-tags='host:*'
```

Drops a noisy tag which shouldn't be present:
```
[filter.noisy-tag]
drop-tags='request_path:*'
```

Drops a subset of metrics by name:
```
[filter.drop-subset]
match-metrics='noisy.*'
exclude-metrics='noisy.butok.*'
drop-metric=true
```
