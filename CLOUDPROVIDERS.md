Configuring cloud providers
--------------------
Cloud providers are a way to automatically enrich metrics with metadata from a cloud vendor.

Cloud providers must be configured through the usage of a configuration file (toml, yaml and json are supported), passed via
`--config-path`.  The cloud provider is specified using the `cloud-provider` configuration option.

There are currently two supported cloud providers:

* `aws` which retrieves tags from AWS instance tags via AWS API calls.
* `k8s` which retrieves tags from kubernetes pod labels and annotations.

All configuration is in a stanza named after the backend, and takes simple key value pairs.

aws
---
### TODO

k8s
---
#### Overview

The k8s cloud provider looks at the IP addresses of incoming metric datagrams and compares that with a list of pods
running in the cluster to determine who sent the metric. If no pod is found with that IP then the metric is sent without
enrichment.

The list of pods is maintained via a watch API operation on the pod resource. This opens a websocket on which any pod
handled by the API server is sent to. This means we get updates as they happen and don't do any active requests to
the API server. This cache is fully resynced every `resync-period` but this is just to ensure total correctness.

#### Important details

`ignore-host` must be set to `false` for the k8s cloud provider to work at all! This is because it works based off the
source IP address of incoming metrics, and these are dropped if `ignore-host=true`.

In kubernetes when a new pod is created it gets a new unique identifier. The k8s cloud provider uses these identifiers
as the hostname for the pods. This means if the k8s cluster has a high pod churn, then there will be a lot of unique
values for the hostname tag. This is not ideal, and will waste resources unless you really care about those hostnames.
To fix this it is highly recommended to drop the hostname from incoming metrics. The
[examples](examples/cloudproviders/k8s/K8S.md) contain the configuration for a filter that will do this.

#### Example with defaults

```$toml
cloud-provider = 'k8s'

[k8s]
kube-api-qps = 5
kube-api-burst = 1.5
# Matches any annotation beginning with gostatsd.atlassian.com/
annotation-tag-regex = '^gostatsd.atlassian.com/(?P<tag>.*)$'
# Matches nothing - no labels included
label-tag-regex = ''
# Set these next two if you're not running inside a kubernetes cluster, or want to use
# a custom role
kubeconfig-context = ''
kubeconfig-path = ''
# This should be overriden via environment variable - see below for example
node-name = ''
resync-period = '5m'
user-agent = 'gostatsd'
watch-cluster = true
```

The configuration settings are as follows:
- `kube-api-qps`: the number of queries per second gostatsd can make to the kubernetes API server before being rate limited
- `kube-api-burst`: the number of queries per second gostatsd can burst above the limit when necessary
- `annotation-tag-regex`: a regex that is compared to every pod's annotations. Any annotations matching the
regex have their value used as a metric tag value. The key of the metric tag is either the entire annotation key, or a subset
matching a named capture group called `tag`
- `label-tag-regex`: like `annotation-tag-regex` but applied to pod labels
- `kubeconfig-context`: specify a kubeconfig context to use to auth to the API server. Must exist within the kubeconfig
file specified by `kubeconfig-path`
- `kubeconfig-path`: path to a [kubeconfig](https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/)
file to use as auth for the cluster. If this is not provided then gostatsd will assume it is running inside a kubernetes
cluster and use in-cluster authentication.
- `node-name`: only used if `watch-cluster` is `false`. The name of the node to watch for pods on. The recommended way
to provide this value is actually via the
[kubernetes downwards API](https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/)
as an environment variable `GSD_K8S_NODE_NAME`. See the kubernetes deployment example for details.
- `resync-period`: how often to completely refresh every entry in the pod watch cache
- `user-agent`: the base user agent used when communicating with the kubernetes API server. The version of the binary
is automatically appended to this string
- `watch-cluster`: if `true` then can enrich metrics from all pods in the cluster. If `false` will only enrich metrics
that are running on the node named `node-name`

#### Tag names and values

The most important options are `annotation-tag-regex` and `label-tag-regex`. These are what allow you to specify what
you care about emitting as tags from your pods.

If the regex matches an annotation/label then it is included as a tag on an emitted metric. If the regex does not match
then the annotation/label is ignored.

Both regexes allow you to specify a named capture group called `tag`. If this capture group exists then the tag name
will be set to the contents of this capture group. If the capture group does not exist, or does not match anything,
then the tag name will be set to the entire annotation/label name.

The value of any included statsd tag is the value of the annotation/label on the pod.

#### Example kubernetes deployments

[See here for example configurations for using the k8s cloud provider in Kubernetes](examples/cloudproviders/k8s/K8S.md).
