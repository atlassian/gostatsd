package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ash2k/stager/wait"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"

	"github.com/atlassian/gostatsd/pkg/cluster/nodes"
)

// Cluster is everything for running a single node in a cluster
type Cluster struct {
	RedisAddr      string
	Namespace      string
	Target         string
	UpdateInterval time.Duration
	ExpiryInterval time.Duration
}

// newCluster will create a new Cluster with default values.
func newCluster() *Cluster {
	local, err := nodes.LocalAddress("1.1.1.1:1")

	if err != nil {
		return nil
	}

	return &Cluster{
		RedisAddr:      "127.0.0.1:6379",
		Namespace:      "namespace",
		Target:         local.String(),
		UpdateInterval: time.Second,
		ExpiryInterval: 4 * time.Second,
	}
}

// AddFlags adds flags for a specific Server to the specified FlagSet.
func (c *Cluster) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&c.RedisAddr, "redis-addr", c.RedisAddr, "Redis address")
	fs.StringVar(&c.Namespace, "namespace", c.Namespace, "Namespace")
	fs.StringVar(&c.Target, "target", c.Target, "Target host to advertise")
	fs.DurationVar(&c.UpdateInterval, "update-interval", c.UpdateInterval, "Cluster update interval")
	fs.DurationVar(&c.ExpiryInterval, "expiry-interval", c.ExpiryInterval, "Cluster expiry interval")
}

// Run runs the specified Cluster.
func (c *Cluster) Run(ctx context.Context) {
	picker := nodes.NewConsistentNodePicker(c.Target, 20)
	rntRunnable := nodes.NewRedisNodeTracker(logrus.StandardLogger(), picker, c.RedisAddr, c.Namespace, c.Target, c.UpdateInterval, c.ExpiryInterval)

	var g wait.Group
	defer g.Wait()
	g.StartWithContext(ctx, rntRunnable)

	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			n := picker.List()
			fmt.Printf("%v\n", n)
		case <-ctx.Done():
			return
		}
	}
}
