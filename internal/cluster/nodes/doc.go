package nodes

/*
Cluster membership is handled with two distinct parts:
- nodes.NodePicker which is notified by an external source of changes to the node list, and returns a single node given a requested key.
- nodes.NodeTracker which performs discovery, and informs a NodePicker of updates.

A node does not have to join the cluster to track the membership, this would allow for a couple of large nodes to be
the aggregation service, and then a fleet of sidecar nodes send to the cluster.  This is probably not going to scale
well, it is just an example, not a recommendation.

The NodeTracker is responsible for not adding duplicates, or removing node twice, so technically it is the source of
truth.
*/
