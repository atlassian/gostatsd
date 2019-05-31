package statsd

import (
	"context"
	"fmt"
	"github.com/ash2k/stager/wait"
	"github.com/atlassian/gostatsd/pkg/ready"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/cluster/nodes"
	"github.com/atlassian/gostatsd/pkg/forwarder"
)

type nodeMap map[string]*gostatsd.MetricMap

func (nm nodeMap) get(key string, forwarded bool) *gostatsd.MetricMap {
	mm, ok := nm[key]
	if !ok {
		mm = gostatsd.NewMetricMap(forwarded)
		nm[key] = mm
	}
	return mm
}

type ClusterHandler struct {
	droppedFail uint64 // atomic
	droppedLoop uint64 // atomic

	// Metrics intended to be processed locally will go to localConsolidator, otherwise they go to remoteConsolidator
	localConsolidator    *gostatsd.MetricConsolidator
	remoteConsolidator   *gostatsd.MetricConsolidator
	chLocalConsolidated  chan []*gostatsd.MetricMap
	chRemoteConsolidated chan []*gostatsd.MetricMap

	logger          logrus.FieldLogger
	localHandler    gostatsd.PipelineHandler
	remoteForwarder forwarder.ClusterForwarder
	picker          nodes.NodePicker
}

// NewClusterHandler initialises a new cluster handler which does clustering
func NewClusterHandler(
	logger logrus.FieldLogger,
	localHandler gostatsd.PipelineHandler,
	remoteForwarder forwarder.ClusterForwarder,
	picker nodes.NodePicker,
) (*ClusterHandler, error) {
	chLocal := make(chan []*gostatsd.MetricMap)
	chRemote := make(chan []*gostatsd.MetricMap)
	return &ClusterHandler{
		localConsolidator:    gostatsd.NewMetricConsolidator(1, false, 100*time.Millisecond, chLocal),
		chLocalConsolidated:  chLocal,
		remoteConsolidator:   gostatsd.NewMetricConsolidator(1, true, 100*time.Millisecond, chRemote),
		chRemoteConsolidated: chRemote,
		logger:               logger,
		localHandler:         localHandler,
		remoteForwarder:      remoteForwarder,
		picker:               picker,
	}, nil
}

// Run will run the ClusterHandler until the context.Context is cancelled
func (ch *ClusterHandler) Run(ctx context.Context) {
	var w wait.Group

	ready.Add(ctx, 2)

	w.StartWithContext(ctx, ch.localConsolidator.Run)
	w.StartWithContext(ctx, ch.remoteConsolidator.Run)
	defer w.Wait()

	ready.SignalReady(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case mms := <-ch.chLocalConsolidated:
			fmt.Printf("dispatch local\n")
			ch.dispatchConsolidated(ctx, mms[0], false)
			fmt.Printf("dispatched local\n")
		case mms := <-ch.chRemoteConsolidated:
			fmt.Printf("dispatch remote\n")
			ch.dispatchConsolidated(ctx, mms[0], true)
			fmt.Printf("dispatched remote\n")
		}
	}
}

// DispatchMetrics will send the metrics to the local consolidator.  Data arriving through this code path will have
// been sent as raw metrics, which is effectively local.
func (ch *ClusterHandler) DispatchMetrics(ctx context.Context, m []*gostatsd.Metric) {
	ch.localConsolidator.ReceiveMetrics(m)
}

// DispatchMetricMap will send local metrics either to the local consolidator or the remote consolidator, depending on
// where the metrics came from.
func (ch *ClusterHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
	if mm.Forwarded {
		ch.remoteConsolidator.ReceiveMetricMap(mm)
	} else {
		ch.localConsolidator.ReceiveMetricMap(mm)
	}
}

// isLoop will return true if it's a forwarded metric, but the target node is remote.
func isLoop(node string, isForwarded bool) bool {
	return isForwarded && (node != nodes.NodeNameSelf)
}

func (ch *ClusterHandler) dispatchConsolidated(ctx context.Context, mm *gostatsd.MetricMap, isForwarded bool) {
	out := nodeMap{}

	droppedFail := uint64(0)
	droppedLoop := uint64(0)

	mm.Counters.Each(func(metricName string, tagsKey string, c gostatsd.Counter) {
		if node, err := ch.picker.Select(metricName + tagsKey); err != nil {
			droppedFail++
		} else if isLoop(node, isForwarded) {
			droppedLoop++
		} else {
			out.get(node, isForwarded).MergeCounter(metricName, tagsKey, c)
		}
	})

	mm.Gauges.Each(func(metricName string, tagsKey string, g gostatsd.Gauge) {
		if node, err := ch.picker.Select(metricName + tagsKey); err != nil {
			droppedFail++
		} else if isLoop(node, isForwarded) {
			droppedLoop++
		} else {
			out.get(node, isForwarded).MergeGauge(metricName, tagsKey, g)
		}
	})

	mm.Timers.Each(func(metricName string, tagsKey string, t gostatsd.Timer) {
		if node, err := ch.picker.Select(metricName + tagsKey); err != nil {
			droppedFail++
		} else if isLoop(node, isForwarded) {
			droppedLoop++
		} else {
			out.get(node, isForwarded).MergeTimer(metricName, tagsKey, t)
		}
	})

	mm.Sets.Each(func(metricName string, tagsKey string, s gostatsd.Set) {
		if node, err := ch.picker.Select(metricName + tagsKey); err != nil {
			droppedFail++
		} else if isLoop(node, isForwarded) {
			droppedLoop++
		} else {
			out.get(node, isForwarded).MergeSet(metricName, tagsKey, s)
		}
	})

	// out is now a map[nodeTarget]metricmap, where nodeTarget may be nodes.NodeNameSelf if it should be dispatched locally
	for node, mm := range out {
		fmt.Printf("assessing %s\n", node)
		if node == nodes.NodeNameSelf {
			fmt.Printf("dispatch mm local\n")
			ch.localHandler.DispatchMetricMap(ctx, mm)
		} else if !isForwarded { // test the flag, just in case
			fmt.Printf("dispatch mm remote to %s\n", node)
			ch.remoteForwarder.DispatchMetricMapTo(ctx, mm, node)
		}
	}

	atomic.AddUint64(&ch.droppedFail, droppedFail)
	atomic.AddUint64(&ch.droppedLoop, droppedLoop)
}

func (ch *ClusterHandler) EstimatedTags() int {
	return ch.localHandler.EstimatedTags()
}

func (ch *ClusterHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	ch.localHandler.DispatchEvent(ctx, e)
}

func (ch *ClusterHandler) WaitForEvents() {
	ch.localHandler.WaitForEvents()
}
