package statsd

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd/pkg/cloudproviders/aws"
	"github.com/atlassian/gostatsd/pkg/cloudproviders/k8s"

	"github.com/atlassian/gostatsd/pkg/cloudproviders"
	"github.com/spf13/viper"

	"github.com/ash2k/stager"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"

	"github.com/ash2k/stager/wait"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type lookupResult struct {
	ip       gostatsd.IP
	instance *gostatsd.Instance // Can be nil if lookup failed or instance was not found
}

type instanceHolder struct {
	lastAccessNano int64
	expires        time.Time          // When this record expires.
	instance       *gostatsd.Instance // Can be nil if the lookup resulted in an error or instance was not found
}

func (ih *instanceHolder) updateAccess() {
	atomic.StoreInt64(&ih.lastAccessNano, time.Now().UnixNano())
}

func (ih *instanceHolder) lastAccess() int64 {
	return atomic.LoadInt64(&ih.lastAccessNano)
}

// CacheOptions holds cache behaviour configuration.
type CacheOptions struct {
	CacheRefreshPeriod        time.Duration
	CacheEvictAfterIdlePeriod time.Duration
	CacheTTL                  time.Duration
	CacheNegativeTTL          time.Duration
}

// CloudHandlerFactory creates a CloudHandler based on some predefined options.
type CloudHandlerFactory struct {
	cloudProviderName string
	version           string
	CloudProvider     gostatsd.CloudProvider
	logger            logrus.FieldLogger
	Limiter           *rate.Limiter
	CacheOptions      *CacheOptions
}

// constructCloudHandlerFactory initialises a new cloud handler factory
func constructCloudHandlerFactory(cloudProviderName string, logger logrus.FieldLogger, cacheOptions CacheOptions, limiter *rate.Limiter, version string) *CloudHandlerFactory {
	return &CloudHandlerFactory{
		CacheOptions:      &cacheOptions,
		logger:            logger,
		Limiter:           limiter,
		CloudProvider:     nil,
		cloudProviderName: cloudProviderName,
		version:           version,
	}
}

// ConstructCloudHandlerFactoryFromViper initialises a new cloud handler factory with defaults based on the name of the cloud
// provider requested.
func ConstructCloudHandlerFactoryFromViper(v *viper.Viper, logger logrus.FieldLogger, version string) (*CloudHandlerFactory, error) {
	cloudProviderName := v.GetString(ParamCloudProvider)
	if cloudProviderName == "" {
		return constructCloudHandlerFactory(cloudProviderName, logger, CacheOptions{}, nil, version), nil
	}

	// Cloud provider defaults
	cpDefaultCacheOpts, ok := DefaultCloudProviderCacheValues[cloudProviderName]
	if !ok {
		return nil, fmt.Errorf("could not find default cache values for cloud provider '%s'", cloudProviderName)
	}
	cpDefaultLimiterOpts, ok := DefaultCloudProviderLimiterValues[cloudProviderName]
	if !ok {
		return nil, fmt.Errorf("could not find default cache values for cloud provider '%s'", cloudProviderName)
	}

	// Set the defaults in Viper based on the cloud provider values before we manipulate things
	v.SetDefault(ParamCacheRefreshPeriod, cpDefaultCacheOpts.CacheRefreshPeriod)
	v.SetDefault(ParamCacheEvictAfterIdlePeriod, cpDefaultCacheOpts.CacheEvictAfterIdlePeriod)
	v.SetDefault(ParamCacheTTL, cpDefaultCacheOpts.CacheTTL)
	v.SetDefault(ParamCacheNegativeTTL, cpDefaultCacheOpts.CacheNegativeTTL)
	v.SetDefault(ParamMaxCloudRequests, cpDefaultLimiterOpts.MaxCloudRequests)
	v.SetDefault(ParamBurstCloudRequests, cpDefaultLimiterOpts.BurstCloudRequests)

	// Set the used values based on the defaults merged with any overrides
	cacheOptions := CacheOptions{
		CacheRefreshPeriod:        v.GetDuration(ParamCacheRefreshPeriod),
		CacheEvictAfterIdlePeriod: v.GetDuration(ParamCacheEvictAfterIdlePeriod),
		CacheTTL:                  v.GetDuration(ParamCacheTTL),
		CacheNegativeTTL:          v.GetDuration(ParamCacheNegativeTTL),
	}
	limiter := rate.NewLimiter(rate.Limit(v.GetInt(ParamMaxCloudRequests)), v.GetInt(ParamBurstCloudRequests))
	return constructCloudHandlerFactory(cloudProviderName, logger, cacheOptions, limiter, version), nil
}

// InitCloudProvider initialises the cloud provider given some already parsed defaults.
// This is mainly split out to enable testing the config parsing separately from the cloud providers.
func (f *CloudHandlerFactory) InitCloudProvider(v *viper.Viper) error {
	options := gostatsd.CloudProviderOptions{Viper: v, Logger: f.logger, Version: f.version}
	switch f.cloudProviderName {
	case aws.ProviderName:
		// AWS has no special options required
		break
	case k8s.ProviderName:
		options.Version = f.version
		nodeName := k8s.GetNodeName()
		options.NodeName = nodeName
	}

	cloudProvider, err := cloudproviders.Init(f.cloudProviderName, options)
	if err != nil {
		return err
	}
	f.CloudProvider = cloudProvider
	return nil
}

// NewCloudHandler creates a new Cloud Handler based on the options set in the CloudHandlerFactory.
func (f *CloudHandlerFactory) NewCloudHandler(handler gostatsd.PipelineHandler) *CloudHandler {
	return NewCloudHandler(f.CloudProvider, handler, f.logger, f.Limiter, f.CacheOptions)
}

// CloudHandler enriches metrics and events with additional information fetched from cloud provider.
type CloudHandler struct {
	// statsCacheHit is accessed by any go routine, must use atomic ops
	statsCacheHit uint64 // Cumulative number of cache hits

	// All other stats fields may only be read or written by the main CloudHandler.Run goroutine
	statsCacheLateHit         uint64 // Cumulative number of late cache hits
	statsCacheMiss            uint64 // Cumulative number of cache misses
	statsCachePositive        uint64 // Absolute number of positive entries in cache
	statsCacheNegative        uint64 // Absolute number of negative entries in cache
	statsCacheRefreshPositive uint64 // Cumulative number of positive refreshes (ie, a refresh which succeeded)
	statsCacheRefreshNegative uint64 // Cumulative number of negative refreshes (ie, a refresh which failed and used old data)
	statsMetricItemsQueued    uint64 // Absolute number of metrics queued, waiting for a CP to respond
	statsMetricHostsQueued    uint64 // Absolute number of IPs waiting for a CP to respond for metrics
	statsEventItemsQueued     uint64 // Absolute number of events queued, waiting for a CP to respond
	statsEventHostsQueued     uint64 // Absolute number of IPs waiting for a CP to respond for events

	// emitChan triggers a write of all the current stats when it is given a Statser
	emitChan chan stats.Statser

	cacheOpts       CacheOptions
	cloud           gostatsd.CloudProvider // Cloud provider interface
	handler         gostatsd.PipelineHandler
	limiter         *rate.Limiter
	metricSource    chan []*gostatsd.Metric
	eventSource     chan *gostatsd.Event
	awaitingEvents  map[gostatsd.IP][]*gostatsd.Event
	awaitingMetrics map[gostatsd.IP][]*gostatsd.Metric
	toLookupIPs     []gostatsd.IP
	wg              sync.WaitGroup

	rw    sync.RWMutex // Protects cache
	cache map[gostatsd.IP]*instanceHolder

	logger logrus.FieldLogger

	estimatedTags int
}

// NewCloudHandler initialises a new cloud handler.
func NewCloudHandler(cloud gostatsd.CloudProvider, handler gostatsd.PipelineHandler, logger logrus.FieldLogger, limiter *rate.Limiter, cacheOptions *CacheOptions) *CloudHandler {
	return &CloudHandler{
		cacheOpts:       *cacheOptions,
		cloud:           cloud,
		handler:         handler,
		limiter:         limiter,
		metricSource:    make(chan []*gostatsd.Metric),
		eventSource:     make(chan *gostatsd.Event),
		emitChan:        make(chan stats.Statser),
		awaitingEvents:  make(map[gostatsd.IP][]*gostatsd.Event),
		awaitingMetrics: make(map[gostatsd.IP][]*gostatsd.Metric),
		cache:           make(map[gostatsd.IP]*instanceHolder),
		estimatedTags:   handler.EstimatedTags() + cloud.EstimatedTags(),
		logger:          logger,
	}
}

// EstimatedTags returns a guess for how many tags to pre-allocate
func (ch *CloudHandler) EstimatedTags() int {
	return ch.estimatedTags
}

func (ch *CloudHandler) DispatchMetrics(ctx context.Context, metrics []*gostatsd.Metric) {
	var toDispatch []*gostatsd.Metric
	var toHandle []*gostatsd.Metric
	for _, m := range metrics {
		if ch.updateTagsAndHostname(m.SourceIP, &m.Tags, &m.Hostname) {
			atomic.AddUint64(&ch.statsCacheHit, 1)
			toDispatch = append(toDispatch, m)
		} else {
			toHandle = append(toHandle, m)
		}
	}

	if len(toDispatch) > 0 {
		ch.handler.DispatchMetrics(ctx, toDispatch)
	}

	if len(toHandle) > 0 {
		select {
		case <-ctx.Done():
		case ch.metricSource <- toHandle:
		}
	}
}

// DispatchMetricMap re-dispatches a metric map through CloudHandler.DispatchMetrics
// TODO: This is inefficient, and should be handled first class, however that is a major re-factor of
//  the CloudHandler.  It is also recommended to not use a CloudHandler in an http receiver based
//  service, as the IP is not propagated.
func (ch *CloudHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
	mm.DispatchMetrics(ctx, ch)
}

func (ch *CloudHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	if ch.updateTagsAndHostname(e.SourceIP, &e.Tags, &e.Hostname) {
		atomic.AddUint64(&ch.statsCacheHit, 1)
		ch.handler.DispatchEvent(ctx, e)
		return
	}
	ch.wg.Add(1) // Increment before sending to the channel
	select {
	case <-ctx.Done():
		ch.wg.Done()
	case ch.eventSource <- e:
	}
}

// WaitForEvents waits for all event-dispatching goroutines to finish.
func (ch *CloudHandler) WaitForEvents() {
	ch.wg.Wait()
	ch.handler.WaitForEvents()
}

func (ch *CloudHandler) RunMetrics(ctx context.Context, statser stats.Statser) {
	if me, ok := ch.cloud.(MetricEmitter); ok {
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
	// regular
	statser.Gauge("cloudprovider.cache_late_hit", float64(ch.statsCacheLateHit), nil)
	statser.Gauge("cloudprovider.cache_miss", float64(ch.statsCacheMiss), nil)
	statser.Gauge("cloudprovider.cache_positive", float64(ch.statsCachePositive), nil)
	statser.Gauge("cloudprovider.cache_negative", float64(ch.statsCacheNegative), nil)
	statser.Gauge("cloudprovider.cache_refresh_positive", float64(ch.statsCacheRefreshPositive), nil)
	statser.Gauge("cloudprovider.cache_refresh_negative", float64(ch.statsCacheRefreshNegative), nil)
	t := gostatsd.Tags{"type:metric"}
	statser.Gauge("cloudprovider.hosts_queued", float64(ch.statsMetricHostsQueued), t)
	statser.Gauge("cloudprovider.items_queued", float64(ch.statsMetricItemsQueued), t)
	t = gostatsd.Tags{"type:event"}
	statser.Gauge("cloudprovider.hosts_queued", float64(ch.statsEventHostsQueued), t)
	statser.Gauge("cloudprovider.items_queued", float64(ch.statsEventItemsQueued), t)
}

func (ch *CloudHandler) Run(ctx context.Context) {
	// IPs to lookup. Can make the channel bigger or smaller but this is the perfect size.
	toLookup := make(chan gostatsd.IP, ch.cloud.MaxInstancesBatch())
	var toLookupC chan<- gostatsd.IP
	var toLookupIP gostatsd.IP

	lookupResults := make(chan *lookupResult)

	ld := lookupDispatcher{
		limiter:       ch.limiter,
		cloud:         ch.cloud,
		toLookup:      toLookup,
		lookupResults: lookupResults,
		logger:        ch.logger,
	}

	// Stager will perform ordered, graceful shutdown. Stage by stage in reverse startup order.
	stgr := stager.New()
	defer stgr.Shutdown() // Wait for CloudProvider and lookupDispatcher to stop

	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // Tell everything to stop

	stage := stgr.NextStageWithContext(ctx)

	// If the cloud provider implements runnable, it has things to do. It must start before anything that
	// accesses it
	if ch.cloud != nil {
		if r, ok := ch.cloud.(gostatsd.Runner); ok {
			stage.StartWithContext(r.Run)
			stage = stgr.NextStageWithContext(ctx)
		}
	}

	stage.StartWithContext(ld.run) // Start the lookup mechanism

	refreshTicker := time.NewTicker(ch.cacheOpts.CacheRefreshPeriod)
	defer refreshTicker.Stop()
	// No locking for ch.cache READ access required - this goroutine owns the object and only it mutates it.
	// So reads from the same goroutine are always safe (no concurrent mutations).
	// When we mutate the cache, we hold the exclusive (write) lock to avoid concurrent reads.
	// When we read from the cache from other goroutines, we obtain the read lock.
	for {
		select {
		case <-ctx.Done():
			return
		case toLookupC <- toLookupIP:
			toLookupIP = gostatsd.UnknownIP
			toLookupC = nil // ip has been sent; if there is nothing to send, will block
		case lr := <-lookupResults:
			ch.handleLookupResult(ctx, lr)
		case t := <-refreshTicker.C:
			ch.doRefresh(ctx, t)
		case metrics := <-ch.metricSource:
			ch.handleMetrics(ctx, metrics)
		case e := <-ch.eventSource:
			ch.handleEvent(ctx, e)
		case statser := <-ch.emitChan:
			ch.emit(statser)
		}
		if toLookupC == nil && len(ch.toLookupIPs) > 0 {
			last := len(ch.toLookupIPs) - 1
			toLookupIP = ch.toLookupIPs[last]
			ch.toLookupIPs[last] = gostatsd.UnknownIP // Enable GC
			ch.toLookupIPs = ch.toLookupIPs[:last]
			toLookupC = toLookup
		}
	}
}

func (ch *CloudHandler) doRefresh(ctx context.Context, t time.Time) {
	var toDelete []gostatsd.IP
	now := t.UnixNano()
	idleNano := ch.cacheOpts.CacheEvictAfterIdlePeriod.Nanoseconds()

	for ip, holder := range ch.cache {
		if now-holder.lastAccess() > idleNano {
			// Entry was not used recently, remove it.
			toDelete = append(toDelete, ip)
			if holder.instance == nil {
				ch.statsCacheNegative--
			} else {
				ch.statsCachePositive--
			}
		} else if t.After(holder.expires) {
			// Entry needs a refresh.
			ch.toLookupIPs = append(ch.toLookupIPs, ip)
		}
	}

	if len(toDelete) > 0 {
		ch.rw.Lock()
		defer ch.rw.Unlock()
		for _, ip := range toDelete {
			delete(ch.cache, ip)
		}
	}
}

func (ch *CloudHandler) handleLookupResult(ctx context.Context, lr *lookupResult) {
	var ttl time.Duration
	if lr.instance == nil {
		ttl = ch.cacheOpts.CacheNegativeTTL
	} else {
		ttl = ch.cacheOpts.CacheTTL
	}
	now := time.Now()
	newHolder := &instanceHolder{
		expires:  now.Add(ttl),
		instance: lr.instance,
	}
	currentHolder := ch.cache[lr.ip]
	if currentHolder == nil {
		// Not in cache, count it
		if lr.instance == nil {
			ch.statsCacheNegative++
		} else {
			ch.statsCachePositive++
		}
		newHolder.lastAccessNano = now.UnixNano()
	} else {
		// In cache, don't count it
		newHolder.lastAccessNano = currentHolder.lastAccess()
		if lr.instance == nil {
			// Use the old instance if there was a lookup error.
			newHolder.instance = currentHolder.instance
			ch.statsCacheRefreshNegative++
		} else {
			if currentHolder.instance == nil && newHolder.instance != nil {
				// An entry has flipped from invalid to valid
				ch.statsCacheNegative--
				ch.statsCachePositive++
			}
			ch.statsCacheRefreshPositive++
		}
	}
	func() {
		ch.rw.Lock()
		defer ch.rw.Unlock()
		ch.cache[lr.ip] = newHolder
	}()
	metrics := ch.awaitingMetrics[lr.ip]
	if metrics != nil {
		delete(ch.awaitingMetrics, lr.ip)
		ch.statsMetricItemsQueued -= uint64(len(metrics))
		ch.statsMetricHostsQueued--
		go ch.updateAndDispatchMetrics(ctx, lr.instance, metrics)
	}
	events := ch.awaitingEvents[lr.ip]
	if events != nil {
		delete(ch.awaitingEvents, lr.ip)
		ch.statsEventItemsQueued -= uint64(len(events))
		ch.statsEventHostsQueued--
		go ch.updateAndDispatchEvents(ctx, lr.instance, events)
	}
}

func (ch *CloudHandler) handleMetrics(ctx context.Context, metrics []*gostatsd.Metric) {
	var toDispatch []*gostatsd.Metric
	for _, m := range metrics {
		holder, ok := ch.cache[m.SourceIP]
		if ok {
			// While metric was in the queue the cache was primed. Use the value.
			ch.statsCacheLateHit++
			holder.updateAccess()
			updateInplace(&m.Tags, &m.Hostname, holder.instance)
			toDispatch = append(toDispatch, m)
		} else {
			// Still nothing in the cache.
			queue := ch.awaitingMetrics[m.SourceIP]
			ch.awaitingMetrics[m.SourceIP] = append(queue, m)
			if len(queue) == 0 {
				// This is the first metric in the queue
				ch.toLookupIPs = append(ch.toLookupIPs, m.SourceIP)
				ch.statsMetricHostsQueued++
			}
			ch.statsMetricItemsQueued++
			ch.statsCacheMiss++
		}
	}

	if len(toDispatch) > 0 {
		go ch.handler.DispatchMetrics(ctx, toDispatch)
	}
}

func (ch *CloudHandler) updateAndDispatchMetrics(ctx context.Context, instance *gostatsd.Instance, metrics []*gostatsd.Metric) {
	for _, m := range metrics {
		updateInplace(&m.Tags, &m.Hostname, instance)
	}
	ch.handler.DispatchMetrics(ctx, metrics)
}

func (ch *CloudHandler) handleEvent(ctx context.Context, e *gostatsd.Event) {
	holder, ok := ch.cache[e.SourceIP]
	if ok {
		// While event was in the queue the cache was primed. Use the value.
		holder.updateAccess()
		ch.statsCacheLateHit++
		go ch.updateAndDispatchEvents(ctx, holder.instance, []*gostatsd.Event{e})
	} else {
		// Still nothing in the cache.
		queue := ch.awaitingEvents[e.SourceIP]
		ch.awaitingEvents[e.SourceIP] = append(queue, e)
		if len(queue) == 0 {
			// This is the first event in the queue
			ch.toLookupIPs = append(ch.toLookupIPs, e.SourceIP)
			ch.statsEventHostsQueued++
		}
		ch.statsEventItemsQueued++
		ch.statsCacheMiss++
	}
}

func (ch *CloudHandler) updateAndDispatchEvents(ctx context.Context, instance *gostatsd.Instance, events []*gostatsd.Event) {
	var dispatched int
	defer func() {
		ch.wg.Add(-dispatched)
	}()
	for _, e := range events {
		updateInplace(&e.Tags, &e.Hostname, instance)
		dispatched++
		ch.handler.DispatchEvent(ctx, e)
	}
}

func (ch *CloudHandler) updateTagsAndHostname(ip gostatsd.IP, tags *gostatsd.Tags, hostname *string) bool {
	instance, ok := ch.getInstance(ip)
	if ok {
		updateInplace(tags, hostname, instance)
	}
	return ok
}

func (ch *CloudHandler) getInstance(ip gostatsd.IP) (*gostatsd.Instance, bool) {
	if ip == gostatsd.UnknownIP {
		return nil, true
	}
	ch.rw.RLock()
	holder, ok := ch.cache[ip]
	ch.rw.RUnlock()
	if ok {
		holder.updateAccess()
		return holder.instance, true
	}
	return nil, false
}

func updateInplace(tags *gostatsd.Tags, hostname *string, instance *gostatsd.Instance) {
	if instance != nil { // It was a positive cache hit (successful lookup cache, not failed lookup cache)
		// Update hostname inplace
		*hostname = instance.ID
		// Update tag list inplace
		*tags = append(*tags, instance.Tags...)
	}
}
