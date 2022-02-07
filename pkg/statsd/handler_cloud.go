package statsd

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ash2k/stager/wait"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"
)

type pendingMetricsAndEvents struct {
	metrics *gostatsd.MetricMap
	events  []*gostatsd.Event
}

// CloudHandler enriches metrics and events with additional information fetched from cloud provider.
type CloudHandler struct {
	// These fields are accessed by any go routine, must use atomic ops
	statsCacheHit  uint64 // Cumulative number of cache hits
	statsCacheMiss uint64 // Cumulative number of cache misses

	cachedInstances gostatsd.CachedInstances
	handler         gostatsd.PipelineHandler
	incomingMetrics chan *gostatsd.MetricMap
	incomingEvents  chan *gostatsd.Event

	// emitChan triggers a write of all the current stats when it is given a Statser
	emitChan chan stats.Statser

	perHostPending  map[gostatsd.Source]*pendingMetricsAndEvents
	toLookupIPs     []gostatsd.Source
	wgPendingEvents sync.WaitGroup

	estimatedTags int
}

// NewCloudHandler initialises a new cloud handler.
func NewCloudHandler(cachedInstances gostatsd.CachedInstances, handler gostatsd.PipelineHandler) *CloudHandler {
	return &CloudHandler{
		cachedInstances: cachedInstances,
		handler:         handler,
		incomingMetrics: make(chan *gostatsd.MetricMap),
		incomingEvents:  make(chan *gostatsd.Event),
		emitChan:        make(chan stats.Statser),
		perHostPending:  make(map[gostatsd.Source]*pendingMetricsAndEvents),
		estimatedTags:   handler.EstimatedTags() + cachedInstances.EstimatedTags(),
	}
}

// EstimatedTags returns a guess for how many tags to pre-allocate
func (ch *CloudHandler) EstimatedTags() int {
	return ch.estimatedTags
}

func (ch *CloudHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
	mmToDispatch := gostatsd.NewMetricMap()
	mmToHandle := gostatsd.NewMetricMap()
	mm.Counters.Each(func(metricName string, tagsKey string, c gostatsd.Counter) {
		if ch.updateTagsAndHostname(&c, c.Source) {
			mmToDispatch.MergeCounter(metricName, gostatsd.FormatTagsKey(c.Source, c.Tags), c)
		} else {
			mmToHandle.MergeCounter(metricName, tagsKey, c)
		}
	})
	mm.Gauges.Each(func(metricName string, tagsKey string, g gostatsd.Gauge) {
		if ch.updateTagsAndHostname(&g, g.Source) {
			mmToDispatch.MergeGauge(metricName, gostatsd.FormatTagsKey(g.Source, g.Tags), g)
		} else {
			mmToHandle.MergeGauge(metricName, tagsKey, g)
		}
	})
	mm.Timers.Each(func(metricName string, tagsKey string, t gostatsd.Timer) {
		if ch.updateTagsAndHostname(&t, t.Source) {
			mmToDispatch.MergeTimer(metricName, gostatsd.FormatTagsKey(t.Source, t.Tags), t)
		} else {
			mmToHandle.MergeTimer(metricName, tagsKey, t)
		}
	})
	mm.Sets.Each(func(metricName string, tagsKey string, s gostatsd.Set) {
		if ch.updateTagsAndHostname(&s, s.Source) {
			mmToDispatch.MergeSet(metricName, gostatsd.FormatTagsKey(s.Source, s.Tags), s)
		} else {
			mmToHandle.MergeSet(metricName, tagsKey, s)
		}
	})

	if !mmToDispatch.IsEmpty() {
		ch.handler.DispatchMetricMap(ctx, mmToDispatch)
	}

	if !mmToHandle.IsEmpty() {
		select {
		case <-ctx.Done():
		case ch.incomingMetrics <- mmToHandle:
		}
	}
}

func (ch *CloudHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	if ch.updateTagsAndHostname(e, e.Source) {
		ch.handler.DispatchEvent(ctx, e)
		return
	}
	ch.wgPendingEvents.Add(1) // Increment before sending to the channel
	select {
	case <-ctx.Done():
		ch.wgPendingEvents.Done()
	case ch.incomingEvents <- e:
	}
}

// WaitForEvents waits for all event-dispatching goroutines to finish.
func (ch *CloudHandler) WaitForEvents() {
	ch.wgPendingEvents.Wait()
	ch.handler.WaitForEvents()
}

func (ch *CloudHandler) RunMetrics(ctx context.Context, statser stats.Statser) {
	if me, ok := ch.cachedInstances.(MetricEmitter); ok {
		var wg wait.Group
		defer wg.Wait()
		wg.Start(func() {
			me.RunMetrics(ctx, statser)
		})
	}

	// All the channels are unbuffered, so no CSWs
	flushed, unregister := statser.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushed:
			ch.scheduleEmit(ctx, statser)
		}
	}
}

// scheduleEmit is used to push a request to the main goroutine requesting metrics
// be emitted.  This is done so we can skip atomic operations on most of our metric
// counters.  In line with the flush notifier, it is fire and forget and won't block
func (ch *CloudHandler) scheduleEmit(ctx context.Context, statser stats.Statser) {
	select {
	case ch.emitChan <- statser:
		// success
	case <-ctx.Done():
		// success-ish
	default:
		// at least we tried
	}
}

func (ch *CloudHandler) emit(statser stats.Statser) {
	// atomic
	statser.Gauge("cloudprovider.cache_hit", float64(atomic.LoadUint64(&ch.statsCacheHit)), nil)
	statser.Gauge("cloudprovider.cache_miss", float64(atomic.LoadUint64(&ch.statsCacheMiss)), nil)
	// non-atomic
	statser.Gauge("cloudprovider.hosts_queued", float64(len(ch.perHostPending)), nil)
}

func (ch *CloudHandler) Run(ctx context.Context) {
	var (
		toLookupC  chan<- gostatsd.Source
		toLookupIP gostatsd.Source
	)
	infoSource := ch.cachedInstances.InfoSource()
	ipSink := ch.cachedInstances.IpSink()
	for {
		select {
		case <-ctx.Done():
			return
		case toLookupC <- toLookupIP:
			toLookupIP = gostatsd.UnknownSource // Enable GC
			toLookupC = nil                     // ip has been sent; if there is nothing to send, will block
		case info := <-infoSource:
			ch.handleInstanceInfo(ctx, info)
		case metrics := <-ch.incomingMetrics:
			ch.handleIncomingMetrics(metrics)
		case e := <-ch.incomingEvents:
			ch.handleIncomingEvent(e)
		case statser := <-ch.emitChan:
			ch.emit(statser)
		}
		if toLookupC == nil && len(ch.toLookupIPs) > 0 {
			last := len(ch.toLookupIPs) - 1
			toLookupIP = ch.toLookupIPs[last]
			ch.toLookupIPs[last] = gostatsd.UnknownSource // Enable GC
			ch.toLookupIPs = ch.toLookupIPs[:last]
			toLookupC = ipSink
		}
	}
}

func (ch *CloudHandler) handleInstanceInfo(ctx context.Context, info gostatsd.InstanceInfo) {
	pending, ok := ch.perHostPending[info.IP]
	if !ok {
		return // got an instance for something we didn't request, ignore it.
	}

	delete(ch.perHostPending, info.IP)
	if pending.metrics != nil {
		go ch.updateAndDispatchMetrics(ctx, info.Instance, pending.metrics)
	}
	if len(pending.events) > 0 {
		go ch.updateAndDispatchEvents(ctx, info.Instance, pending.events)
	}
}

// preparePending will return a place to queue things that are waiting to be processed,
// and ensure that source will be looked up if it wasn't already.
func (ch *CloudHandler) preparePending(source gostatsd.Source) *pendingMetricsAndEvents {
	if _, ok := ch.perHostPending[source]; !ok {
		ch.perHostPending[source] = &pendingMetricsAndEvents{}
		ch.toLookupIPs = append(ch.toLookupIPs, source)
	}
	return ch.perHostPending[source]
}

// prepareMetricQueue will ensure that ch.perHostPending has a matching MetricMap for
// the provided source and return it.
func (ch *CloudHandler) prepareMetricQueue(source gostatsd.Source) *gostatsd.MetricMap {
	queue := ch.preparePending(source)
	if queue.metrics == nil {
		// There might be value in pushing this to preparePending, since the split is
		// really only beneficial if a host is only sending events and not metrics, and
		// this adds an extra comparison to every lookup.
		queue.metrics = gostatsd.NewMetricMap()
	}
	return queue.metrics
}

func (ch *CloudHandler) handleIncomingMetrics(mm *gostatsd.MetricMap) {
	// The <metric>.Source values could be from different hosts if they were
	// forwarded, therefore we need to do a lookup each time.
	mm.Counters.Each(func(metricName string, tagsKey string, c gostatsd.Counter) {
		ch.prepareMetricQueue(c.Source).MergeCounter(metricName, tagsKey, c)
	})
	mm.Gauges.Each(func(metricName string, tagsKey string, g gostatsd.Gauge) {
		ch.prepareMetricQueue(g.Source).MergeGauge(metricName, tagsKey, g)
	})
	mm.Sets.Each(func(metricName string, tagsKey string, s gostatsd.Set) {
		ch.prepareMetricQueue(s.Source).MergeSet(metricName, tagsKey, s)
	})
	mm.Timers.Each(func(metricName string, tagsKey string, t gostatsd.Timer) {
		ch.prepareMetricQueue(t.Source).MergeTimer(metricName, tagsKey, t)
	})
}

func (ch *CloudHandler) handleIncomingEvent(e *gostatsd.Event) {
	queue := ch.preparePending(e.Source)
	queue.events = append(queue.events, e)
}

func (ch *CloudHandler) updateAndDispatchMetrics(ctx context.Context, instance *gostatsd.Instance, mmIn *gostatsd.MetricMap) {
	mmOut := gostatsd.NewMetricMap()
	mmIn.Counters.Each(func(metricName string, tagsKey string, c gostatsd.Counter) {
		updateInplace(&c, instance)
		mmOut.MergeCounter(metricName, gostatsd.FormatTagsKey(c.Source, c.Tags), c)
	})
	mmIn.Gauges.Each(func(metricName string, tagsKey string, g gostatsd.Gauge) {
		updateInplace(&g, instance)
		mmOut.MergeGauge(metricName, gostatsd.FormatTagsKey(g.Source, g.Tags), g)
	})
	mmIn.Sets.Each(func(metricName string, tagsKey string, s gostatsd.Set) {
		updateInplace(&s, instance)
		mmOut.MergeSet(metricName, gostatsd.FormatTagsKey(s.Source, s.Tags), s)
	})
	mmIn.Timers.Each(func(metricName string, tagsKey string, t gostatsd.Timer) {
		updateInplace(&t, instance)
		mmOut.MergeTimer(metricName, gostatsd.FormatTagsKey(t.Source, t.Tags), t)
	})
	ch.handler.DispatchMetricMap(ctx, mmOut)
}

func (ch *CloudHandler) updateAndDispatchEvents(ctx context.Context, instance *gostatsd.Instance, events []*gostatsd.Event) {
	var dispatched int
	defer func() {
		ch.wgPendingEvents.Add(-dispatched)
	}()
	for _, e := range events {
		updateInplace(e, instance)
		dispatched++
		ch.handler.DispatchEvent(ctx, e)
	}
}

func (ch *CloudHandler) updateTagsAndHostname(obj TagChanger, source gostatsd.Source) bool /*is a cache hit*/ {
	instance, cacheHit := ch.getInstance(source)
	if cacheHit {
		updateInplace(obj, instance)
	}
	return cacheHit
}

func (ch *CloudHandler) getInstance(ip gostatsd.Source) (*gostatsd.Instance, bool /*is a cache hit*/) {
	if ip == gostatsd.UnknownSource {
		return nil, true
	}
	instance, cacheHit := ch.cachedInstances.Peek(ip)
	if !cacheHit {
		atomic.AddUint64(&ch.statsCacheMiss, 1)
		return nil, false
	}
	atomic.AddUint64(&ch.statsCacheHit, 1)
	return instance, true
}

func updateInplace(obj TagChanger, instance *gostatsd.Instance) {
	if instance != nil { // It was a positive cache hit (successful lookup cache, not failed lookup cache)
		obj.AddTagsSetSource(instance.Tags, instance.ID)
	}
}
