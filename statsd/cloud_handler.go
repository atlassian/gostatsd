package statsd

import (
	"sync"
	"sync/atomic"
	"time"

	cloudTypes "github.com/atlassian/gostatsd/cloudprovider/types"
	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
)

const (
	lookupChannelSize = 1024 // Random size. Should be good enough.
	// DefaultCacheRefreshPeriod is the default cache refresh period.
	DefaultCacheRefreshPeriod = 1 * time.Minute
	// DefaultCacheEvictAfterIdlePeriod is the default idle cache eviction period.
	DefaultCacheEvictAfterIdlePeriod = 10 * time.Minute
	// DefaultCacheTTL is the default cache TTL for successful lookups.
	DefaultCacheTTL = 30 * time.Minute
	// DefaultCacheNegativeTTL is the default cache TTL for failed lookups (errors or when instance was not found).
	DefaultCacheNegativeTTL = 1 * time.Minute
)

type lookupResult struct {
	ip       types.IP
	instance *cloudTypes.Instance // Can be nil if lookup failed
}

type instanceHolder struct {
	lastAccessNano int64
	expires        time.Time            // When this record expires.
	instance       *cloudTypes.Instance // Can be nil if the lookup resulted in an error (instance not found/etc)
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

// cloudHandler enriches metrics and events with additional information fetched from cloud provider.
type cloudHandler struct {
	cacheOpts    CacheOptions
	cloud        cloudTypes.Interface // Cloud provider interface
	next         Handler
	limiter      *rate.Limiter
	metricSource chan *types.Metric
	eventSource  chan *types.Event

	rw    sync.RWMutex // Protects cache
	cache map[types.IP]*instanceHolder
}

// NewCloudHandler initialises a new cloud handler.
// If cacheOptions is nil default cache configuration is used.
func NewCloudHandler(cloud cloudTypes.Interface, next Handler, limiter *rate.Limiter, cacheOptions *CacheOptions) RunableHandler {
	if cacheOptions == nil {
		cacheOptions = &CacheOptions{
			CacheRefreshPeriod:        DefaultCacheRefreshPeriod,
			CacheEvictAfterIdlePeriod: DefaultCacheEvictAfterIdlePeriod,
			CacheTTL:                  DefaultCacheTTL,
			CacheNegativeTTL:          DefaultCacheNegativeTTL,
		}
	}
	return &cloudHandler{
		cacheOpts:    *cacheOptions,
		cloud:        cloud,
		next:         next,
		limiter:      limiter,
		metricSource: make(chan *types.Metric),
		eventSource:  make(chan *types.Event),
		cache:        make(map[types.IP]*instanceHolder),
	}
}

func (ch *cloudHandler) DispatchMetric(ctx context.Context, m *types.Metric) error {
	if ch.updateTagsAndHostname(m.SourceIP, &m.Tags, &m.Hostname) {
		return ch.next.DispatchMetric(ctx, m)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch.metricSource <- m:
		return nil
	}
}

func (ch *cloudHandler) DispatchEvent(ctx context.Context, e *types.Event) error {
	if ch.updateTagsAndHostname(e.SourceIP, &e.Tags, &e.Hostname) {
		return ch.next.DispatchEvent(ctx, e)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch.eventSource <- e:
		return nil
	}
}

func (ch *cloudHandler) Run(ctx context.Context) error {
	toLookup := make(chan types.IP, lookupChannelSize) // IPs to lookup
	lookupResults := make(chan *lookupResult)
	awaitingEvents := make(map[types.IP][]*types.Event)
	awaitingMetrics := make(map[types.IP][]*types.Metric)

	var wg sync.WaitGroup
	defer wg.Wait()       // Wait for lookupDispatcher to stop
	defer close(toLookup) // Tell lookupDispatcher to stop
	wg.Add(1)
	go ch.lookupDispatcher(ctx, &wg, toLookup, lookupResults)

	refreshTicker := time.NewTicker(ch.cacheOpts.CacheRefreshPeriod)
	defer refreshTicker.Stop()
	// No locking for ch.cache READ access required - this goroutine owns the object and only it mutates it.
	// So reads from the same goroutine are always safe (no concurrent mutations).
	// When we mutate the cache, we hold the exclusive (write) lock to avoid concurrent reads.
	// When we read from the cache from other goroutines, we obtain the read lock.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case lr := <-lookupResults:
			ch.handleLookupResult(ctx, lr, awaitingMetrics, awaitingEvents)
		case t := <-refreshTicker.C:
			ch.doRefresh(ctx, toLookup, t)
		case m := <-ch.metricSource:
			ch.handleMetric(ctx, toLookup, m, awaitingMetrics)
		case e := <-ch.eventSource:
			ch.handleEvent(ctx, toLookup, e, awaitingEvents)
		}
	}
}

func (ch *cloudHandler) doRefresh(ctx context.Context, toLookup chan<- types.IP, t time.Time) {
	var toDelete []types.IP
	now := t.UnixNano()
	idleNano := ch.cacheOpts.CacheEvictAfterIdlePeriod.Nanoseconds()
	for ip, holder := range ch.cache {
		if now-holder.lastAccess() > idleNano {
			// Entry was not used recently, remove it.
			toDelete = append(toDelete, ip)
		} else if t.After(holder.expires) {
			// Entry needs a refresh.
			select {
			case <-ctx.Done():
				return
			case toLookup <- ip:
			}
		}
	}
	if len(toDelete) > 0 {
		ch.rw.Lock()
		for _, ip := range toDelete {
			delete(ch.cache, ip)
		}
		ch.rw.Unlock()
	}
}

func (ch *cloudHandler) handleLookupResult(ctx context.Context, lr *lookupResult, awaitingMetrics map[types.IP][]*types.Metric, awaitingEvents map[types.IP][]*types.Event) {
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
		newHolder.lastAccessNano = now.UnixNano()
	} else {
		newHolder.lastAccessNano = currentHolder.lastAccess()
	}
	ch.rw.Lock()
	ch.cache[lr.ip] = newHolder
	ch.rw.Unlock()
	metrics := awaitingMetrics[lr.ip]
	if metrics != nil {
		delete(awaitingMetrics, lr.ip)
		go ch.updateAndDispatchMetrics(ctx, lr.instance, metrics...)
	}
	events := awaitingEvents[lr.ip]
	if events != nil {
		delete(awaitingEvents, lr.ip)
		go ch.updateAndDispatchEvents(ctx, lr.instance, events...)
	}
}

func (ch *cloudHandler) handleMetric(ctx context.Context, toLookup chan<- types.IP, m *types.Metric, awaitingMetrics map[types.IP][]*types.Metric) {
	holder, ok := ch.cache[m.SourceIP]
	if ok {
		// While metric was in the queue the cache was primed. Use the value.
		holder.updateAccess()
		go ch.updateAndDispatchMetrics(ctx, holder.instance, m)
	} else {
		// Still nothing in the cache.
		queue := awaitingMetrics[m.SourceIP]
		awaitingMetrics[m.SourceIP] = append(queue, m)
		if len(queue) == 0 {
			// This is the first metric in the queue
			select {
			case <-ctx.Done():
			case toLookup <- m.SourceIP:
			}
		}
	}
}

func (ch *cloudHandler) updateAndDispatchMetrics(ctx context.Context, instance *cloudTypes.Instance, metrics ...*types.Metric) {
	for _, m := range metrics {
		updateInplace(&m.Tags, &m.Hostname, instance)
		if err := ch.next.DispatchMetric(ctx, m); err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				return
			}
			log.Warnf("Failed to dispatch metric: %v", err)
		}
	}
}

func (ch *cloudHandler) handleEvent(ctx context.Context, toLookup chan<- types.IP, e *types.Event, awaitingEvents map[types.IP][]*types.Event) {
	holder, ok := ch.cache[e.SourceIP]
	if ok {
		// While event was in the queue the cache was primed. Use the value.
		holder.updateAccess()
		go ch.updateAndDispatchEvents(ctx, holder.instance, e)
	} else {
		// Still nothing in the cache.
		queue := awaitingEvents[e.SourceIP]
		awaitingEvents[e.SourceIP] = append(queue, e)
		if len(queue) == 0 {
			// This is the first event in the queue
			select {
			case <-ctx.Done():
			case toLookup <- e.SourceIP:
			}
		}
	}
}

func (ch *cloudHandler) updateAndDispatchEvents(ctx context.Context, instance *cloudTypes.Instance, events ...*types.Event) {
	for _, e := range events {
		updateInplace(&e.Tags, &e.Hostname, instance)
		if err := ch.next.DispatchEvent(ctx, e); err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				return
			}
			log.Warnf("Failed to dispatch event: %v", err)
		}
	}
}

func (ch *cloudHandler) lookupDispatcher(ctx context.Context, wg *sync.WaitGroup, toLookup <-chan types.IP, lookupResults chan<- *lookupResult) {
	defer wg.Done()
	defer log.Info("Cloud lookup dispatcher stopped")

	var wgLookups sync.WaitGroup
	defer wgLookups.Wait() // Wait for all in-flight lookups to finish

	for ip := range toLookup {
		if err := ch.limiter.Wait(ctx); err != nil {
			if err != context.Canceled && err != context.DeadlineExceeded {
				// This could be an error caused by context signaling done. Or something nasty but it is very unlikely.
				log.Warnf("Error from limiter: %v", err)
			}
			return
		}
		wgLookups.Add(1)
		go ch.doLookup(ctx, &wgLookups, ip, lookupResults)
	}
}

func (ch *cloudHandler) doLookup(ctx context.Context, wg *sync.WaitGroup, ip types.IP, lookupResults chan<- *lookupResult) {
	defer wg.Done()

	instance, err := ch.cloud.Instance(ip)
	if err != nil {
		log.Debugf("Error retrieving instance details from cloud provider for %s: %v", ip, err)
	}
	res := &lookupResult{
		ip:       ip,
		instance: instance,
	}
	select {
	case <-ctx.Done():
	case lookupResults <- res:
	}
}

func (ch *cloudHandler) updateTagsAndHostname(ip types.IP, tags *types.Tags, hostname *string) bool {
	instance, ok := ch.getInstance(ip)
	if ok {
		updateInplace(tags, hostname, instance)
	}
	return ok
}

func (ch *cloudHandler) getInstance(ip types.IP) (*cloudTypes.Instance, bool) {
	ch.rw.RLock()
	holder, ok := ch.cache[ip]
	ch.rw.RUnlock()
	if ok {
		holder.updateAccess()
		return holder.instance, true
	}
	return nil, false
}

func updateInplace(tags *types.Tags, hostname *string, instance *cloudTypes.Instance) {
	if instance != nil { // It was a positive cache hit (successful lookup cache, not failed lookup cache)
		// Update hostname inplace
		*hostname = instance.ID
		// Update tag list inplace
		*tags = append(*tags, "region:"+instance.Region)
		*tags = append(*tags, instance.Tags...)
	}
}
