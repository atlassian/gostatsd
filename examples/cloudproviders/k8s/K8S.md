Running the k8s cloud provider in Kubernetes
--------------------------------------------

For large clusters it is highly recommended to use the "one gostatsd per node" approach below, as this will distribute
CPU and network load across the nodes in your cluster.

#### RBAC Permissions

The k8s cloud provider requires certain cluster permissions to operate. The
[RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) permissions required are in
[rbac.yaml](rbac.yaml).

#### One gostatsd for a cluster

[deployment.yaml](deployment.yaml) shows an example configuration for running one gostatsd that enriches metrics for
an entire cluster. Pods can send metrics to `gostatsd.default.svc.cluster.local:8125` and they will be enriched and
processed.

#### One gostatsd per node

[daemonset.yaml](daemonset.yaml) shows an example configuration for running a gostatsd on each node in the cluster.
This gostatsd is only able to enrich metrics that come from pods running on the same node as the gostatsd. We pass the
node name into gostatsd via the downwards API and the `GSD_K8S_NODE_NAME` variable.

Pods running on the node are responsible for knowing how to send metrics to the node itself. The `daemonset.yaml` also
contains an example application which sends metrics to it's own node every 10 seconds, using the downward API to find
the IP address of the node.

**This is the preferred configuration for large kubernetes clusters as it will distribute CPU load across the cluster, and reduce network load.** 