package nodes

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"github.com/tilinna/clock"
)

type redisNodeTracker struct {
	logger logrus.FieldLogger

	picker NodePicker

	client    RedisClient
	namespace string
	nodeId    string
	nodes     map[string]time.Time

	updateInterval time.Duration
	expiryInterval time.Duration
}

type RedisClient interface {
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
	Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd
}

// NewRedisNodeTracker returns a NodeTracker which tracks nodes in a Redis, and updates the provided NodePicker with
// lifecycle events.
//
// Note that we're not trying to solve the CAP theorem here, if Redis has a bad time, then so do we.
func NewRedisNodeTracker(
	logger logrus.FieldLogger,
	picker NodePicker,
	redisClient RedisClient,
	namespace, nodeId string,
	updateInterval, expiryInterval time.Duration,
) NodeTracker {
	return &redisNodeTracker{
		logger: logger,
		picker: picker,

		client:    redisClient,
		namespace: namespace,
		nodeId:    nodeId,
		nodes:     make(map[string]time.Time),

		updateInterval: updateInterval,
		expiryInterval: expiryInterval,
	}
}

// Run will track nodes via Redis PubSub until the context is closed.
func (rnt *redisNodeTracker) Run(ctx context.Context) {
	clck := clock.FromContext(ctx)

	pubsub := rnt.client.Subscribe(ctx, rnt.namespace)
	defer pubsub.Close()

	psChan := pubsub.Channel() // Closed when pubsub is Closed

	// Send an immediate heartbeat, this will also solicit other nodes to respond.
	if err := rnt.sendIntroductionRequest(ctx); err != nil {
		rnt.logger.WithError(err).Warning("Initial redis check in failed")
	}

	// Starting the ticker is how we signal to tests that everything is ready to go.
	ticker := clck.NewTicker(rnt.updateInterval)
	defer ticker.Stop()

	defer func() {
		// On shutdown, remove ourselves from other hosts + the NodePicker.
		ctxExit, cancel := clck.TimeoutContext(context.Background(), 1*time.Second)
		rnt.sendDrop(ctxExit)
		cancel()
		if rnt.nodeId != "" {
			rnt.picker.Remove(rnt.nodeId)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rnt.expireNodes(clck.Now())
			if err := rnt.sendHeartbeat(ctx); err != nil {
				rnt.logger.WithError(err).Warning("Failed to check in to redis")
			}
		case msg := <-psChan:
			rnt.handleMessage(ctx, msg.Payload, clck.Now())
		}
	}
}

func (rnt *redisNodeTracker) handleMessage(ctx context.Context, message string, now time.Time) {
	if message == "" {
		return
	}
	node := message[1:]
	switch message[0] {
	case '-':
		rnt.dropNode(node)
	case '?':
		rnt.refreshNode(node, now)
		if node != rnt.nodeId {
			// It's not us, and it wants to know about us, send a broadcast out to let it know we exist.
			if err := rnt.sendHeartbeat(ctx); err != nil {
				rnt.logger.WithError(err).WithField("newNode", node).Warning("Failed to send introduction reply")
			}
		}
	case '+':
		rnt.refreshNode(node, now)
	}
}

// refreshNode will attempt to update the expiry on an existing node, if it
// doesn't exist, it will be added to the NodePicker.  Returns true if this
// is a new node.
func (rnt *redisNodeTracker) refreshNode(nodeId string, now time.Time) bool {
	// Does not talk to Redis

	// Only add it to the picker if it wasn't already being tracked, in case the Picker doesn't de-dupe.
	_, existingNode := rnt.nodes[nodeId]
	if !existingNode {
		rnt.logger.WithField("node", nodeId).Info("Added node")
		rnt.picker.Add(nodeId)
	}

	rnt.nodes[nodeId] = now.Add(rnt.expiryInterval)
	return !existingNode
}

// dropNode will drop the node from the tracked nodes.
func (rnt *redisNodeTracker) dropNode(nodeId string) {
	// Does not talk to Redis

	_, ok := rnt.nodes[nodeId]
	if ok {
		rnt.logger.WithField("node", nodeId).Info("Removing node")
		rnt.picker.Remove(nodeId)
		delete(rnt.nodes, nodeId)
	}
}

// expireNodes will expire nodes which have not updated recently enough.
func (rnt *redisNodeTracker) expireNodes(now time.Time) {
	// Does not talk to Redis
	for nodeId, expiry := range rnt.nodes {
		if now.After(expiry) {
			rnt.logger.WithField("node", nodeId).Info("Expired node")
			rnt.picker.Remove(nodeId)
			delete(rnt.nodes, nodeId)
		}
	}
}

// sendIntroductionRequest will announce the presence of this node to the PubSub endpoint, if the nodeId is configured.
//
// It is different to sendHeartbeat in that it is an explicit request for everyone else to respond with a heartbeat.
func (rnt *redisNodeTracker) sendIntroductionRequest(ctx context.Context) error {
	// Talks to redis
	if rnt.nodeId != "" {
		cmd := rnt.client.Publish(ctx, rnt.namespace, "?"+rnt.nodeId)
		return cmd.Err()
	} else {
		return nil
	}
}

// sendHeartbeat will announce the presence of this node to the PubSub endpoint, if the nodeId is configured.
func (rnt *redisNodeTracker) sendHeartbeat(ctx context.Context) error {
	// Talks to redis
	if rnt.nodeId != "" {
		cmd := rnt.client.Publish(ctx, rnt.namespace, "+"+rnt.nodeId)
		return cmd.Err()
	} else {
		return nil
	}
}

// sendDrop will announce to the PubSub endpoint that this node is going away, if the nodeId is configured.
func (rnt *redisNodeTracker) sendDrop(ctx context.Context) {
	// Talks to redis
	if rnt.nodeId != "" {
		rnt.client.Publish(ctx, rnt.namespace, "-"+rnt.nodeId)
	}
}
