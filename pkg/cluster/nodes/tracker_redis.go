package nodes

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"

	"github.com/atlassian/gostatsd"
)

type redisNodeTracker struct {
	logger logrus.FieldLogger

	picker NodePicker

	client    *redis.Client
	namespace string
	nodeId    string
	nodes     map[string]time.Time

	updateInterval time.Duration
	expiryInterval time.Duration

	// for testing
	now func() time.Time
}

// NewRedisNodeTracker returns a gostatsd.Runner which tracks nodes in Redis, and updates the provided NodePicker with
// lifecycle events
func NewRedisNodeTracker(
	logger logrus.FieldLogger,
	picker NodePicker,
	redisAddr, namespace, nodeId string,
	updateInterval, expiryInterval time.Duration,
) gostatsd.Runnable {
	options := &redis.Options{
		Addr: redisAddr,
		DB:   0,
	}

	return (&redisNodeTracker{
		logger: logger,
		picker: picker,

		client:    redis.NewClient(options),
		namespace: namespace,
		nodeId:    nodeId,
		nodes:     map[string]time.Time{},

		updateInterval: updateInterval,
		expiryInterval: expiryInterval,

		now: time.Now,
	}).Run
}

// Run will track nodes via Redis PubSub until the context is closed.
func (rnt *redisNodeTracker) Run(ctx context.Context) {
	pubsub := rnt.client.Subscribe(rnt.namespace)
	defer pubsub.Close()

	ticker := time.NewTicker(rnt.updateInterval)
	defer ticker.Stop()

	psChan := pubsub.Channel() // Closed when pubsub is Closed
	defer rnt.dropPresence()

	_ = rnt.refreshPresence()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rnt.expireNodes()
			err := rnt.refreshPresence()
			if err != nil {
				rnt.logger.Warning("Failed to check in to cluster")
			}
		case msg := <-psChan:
			rnt.handleMessage(msg.Payload)
		}
	}
}

func (rnt *redisNodeTracker) handleMessage(message string) {
	if message == "" {
		return
	}
	switch message[0] {
	case '-':
		rnt.dropNode(message[1:])
	case '+':
		rnt.refreshNode(message[1:])
	}
}

// expireNodes will expire nodes which have not updated recently enough.
func (rnt *redisNodeTracker) expireNodes() {
	// Does not talk to Redis
	now := rnt.now()

	for nodeId, expiry := range rnt.nodes {
		if now.After(expiry) {
			rnt.picker.Remove(nodeId)
			delete(rnt.nodes, nodeId)
		}
	}
}

// refreshNode will attempt to update the expiry on an existing node, if it
// doesn't exist, it will be added.
func (rnt *redisNodeTracker) refreshNode(nodeId string) {
	// Does not talk to Redis

	// Only add it to the picker if it wasn't already being tracked, in case the Picker doesn't de-dupe.
	if _, ok := rnt.nodes[nodeId]; !ok {
		rnt.picker.Add(nodeId)
	}

	rnt.nodes[nodeId] = rnt.now().Add(rnt.expiryInterval)
}

// dropNode will drop the node from the tracked nodes.
func (rnt *redisNodeTracker) dropNode(nodeId string) {
	// Does not talk to Redis

	fmt.Printf("Dropping node %s\n", nodeId)
	_, ok := rnt.nodes[nodeId]
	if ok {
		rnt.picker.Remove(nodeId)
		delete(rnt.nodes, nodeId)
	}
}

// refreshPresence will announce the presence of this node to the PubSub endpoint, if the nodeId is configured.
func (rnt *redisNodeTracker) refreshPresence() error {
	// Talks to redis
	if rnt.nodeId != "" {
		cmd := rnt.client.Publish(rnt.namespace, "+"+rnt.nodeId)
		return cmd.Err()
	} else {
		return nil
	}
}

// dropPresence will announce the absence of this node to the PubSub endpoint, if the nodeId is configured.
func (rnt *redisNodeTracker) dropPresence() {
	// Talks to redis
	if rnt.nodeId != "" {
		rnt.client.Publish(rnt.namespace, "-"+rnt.nodeId)
	}
}
