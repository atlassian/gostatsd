package cloudprovider

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ash2k/stager/wait"
	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

func NewCachedCloudProvider(logger logrus.FieldLogger, limiter *rate.Limiter, cloudProvider gostatsd.CloudProvider, cacheOpts gostatsd.CacheOptions) *CachedCloudProvider {
	return &CachedCloudProvider{
		logger:         logger,
		limiter:        limiter,
		cloudProvider:  cloudProvider,
		cacheOpts:      cacheOpts,
		ipSinkSource:   make(chan gostatsd.IP),
		infoSinkSource: make(chan gostatsd.InstanceInfo),
		emitChan:       make(chan stats.Statser),
		cache:          make(map[gostatsd.IP]*instanceHolder),
	}
}

type CachedCloudProvider struct {
	// These fields may only be read or written by the main CachedCloudProvider.Run goroutine
	statsCacheRefreshPositive uint64 // Cumulative number of positive refreshes (ie, a refresh which succeeded)
	statsCacheRefreshNegative uint64 // Cumulative number of negative refreshes (ie, a refresh which failed and used old data)
	statsCachePositive        uint64 // Absolute number of positive entries in cache
	statsCacheNegative        uint64 // Absolute number of negative entries in cache

	logger         logrus.FieldLogger
	limiter        *rate.Limiter
	cloudProvider  gostatsd.CloudProvider
	cacheOpts      gostatsd.CacheOptions
	ipSinkSource   chan gostatsd.IP
	infoSinkSource chan gostatsd.InstanceInfo

	// emitChan triggers a write of all the current stats when it is given a Statser
	emitChan     chan stats.Statser
	rw           sync.RWMutex // Protects cache
	cache        map[gostatsd.IP]*instanceHolder
	toLookupIPs  []gostatsd.IP
	toReturnInfo []gostatsd.InstanceInfo
}

func (ccp *CachedCloudProvider) Run(ctx context.Context) {
	var (
		toLookupC     chan<- gostatsd.IP
		toLookupIP    gostatsd.IP
		toReturnInfoC chan<- gostatsd.InstanceInfo
		toReturnInfo  gostatsd.InstanceInfo
		wg            wait.Group
	)
	// this goroutine needs to populate/update the cache so an intermediate InstanceInfo channel is used below that allows
	// to intercept, update the cache and then push the information through to the cache consumer.
	ownInfoSource := make(chan gostatsd.InstanceInfo)
	ld := cloudProviderLookupDispatcher{
		logger:        ccp.logger,
		limiter:       ccp.limiter,
		cloudProvider: ccp.cloudProvider,
		ipSource:      ccp.ipSinkSource, // our sink is their source
		infoSink:      ownInfoSource,    // their sink is our source
	}

	defer wg.Wait() // Wait for cloudProviderLookupDispatcher to stop

	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // Tell cloudProviderLookupDispatcher to stop

	wg.StartWithContext(ctx, ld.run)

	refreshTicker := time.NewTicker(ccp.cacheOpts.CacheRefreshPeriod)
	defer refreshTicker.Stop()
	// No locking for ccp.cache READ access required - this goroutine owns the object and only it mutates it.
	// So reads from the same goroutine are always safe (no concurrent mutations).
	// When we mutate the cache, we hold the exclusive (write) lock to avoid concurrent reads.
	// When we read from the cache from other goroutines in the Peek() method, we obtain the read lock.
	for {
		select {
		case <-ctx.Done():
			return
		case toLookupC <- toLookupIP:
			toLookupIP = gostatsd.UnknownIP // enable GC
			toLookupC = nil                 // ip has been sent; if there is nothing to send, the case is disabled
		case toReturnInfoC <- toReturnInfo:
			toReturnInfo = gostatsd.InstanceInfo{} // enable GC
			toReturnInfoC = nil                    // info has been sent; if there is nothing to send, the case is disabled
		case info := <-ownInfoSource:
			ccp.handleInstanceInfo(info)
		case t := <-refreshTicker.C:
			ccp.doRefresh(t)
		case statser := <-ccp.emitChan:
			ccp.emit(statser)
		}
		if toLookupC == nil && len(ccp.toLookupIPs) > 0 {
			last := len(ccp.toLookupIPs) - 1
			toLookupIP = ccp.toLookupIPs[last]
			ccp.toLookupIPs[last] = gostatsd.UnknownIP // enable GC
			ccp.toLookupIPs = ccp.toLookupIPs[:last]
			toLookupC = ccp.ipSinkSource
		}
		if toReturnInfoC == nil && len(ccp.toReturnInfo) > 0 {
			last := len(ccp.toReturnInfo) - 1
			toReturnInfo = ccp.toReturnInfo[last]
			ccp.toReturnInfo[last] = gostatsd.InstanceInfo{} // enable GC
			ccp.toReturnInfo = ccp.toReturnInfo[:last]
			toReturnInfoC = ccp.infoSinkSource
		}
	}
}

func (ccp *CachedCloudProvider) Peek(ip gostatsd.IP) (*gostatsd.Instance, bool /*is a cache hit*/) {
	ccp.rw.RLock()
	holder, existsInCache := ccp.cache[ip]
	ccp.rw.RUnlock()
	if !existsInCache {
		return nil, false
	}
	holder.updateAccess()
	return holder.instance, true // can be nil, true
}

func (ccp *CachedCloudProvider) IpSink() chan<- gostatsd.IP {
	return ccp.ipSinkSource
}

func (ccp *CachedCloudProvider) InfoSource() <-chan gostatsd.InstanceInfo {
	return ccp.infoSinkSource
}

func (ccp *CachedCloudProvider) EstimatedTags() int {
	return ccp.cloudProvider.EstimatedTags()
}

func (ccp *CachedCloudProvider) RunMetrics(ctx context.Context, statser stats.Statser) {
	// All the channels are unbuffered, so no CSWs
	flushed, unregister := statser.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushed:
			ccp.scheduleEmit(ctx, statser)
		}
	}
}

// scheduleEmit is used to push a request to the main goroutine requesting metrics
// be emitted.  This is done so we can skip atomic operations on most of our metric
// counters.  In line with the flush notifier, it is fire and forget and won't block
func (ccp *CachedCloudProvider) scheduleEmit(ctx context.Context, statser stats.Statser) {
	select {
	case ccp.emitChan <- statser:
		// success
	case <-ctx.Done():
		// success-ish
	default:
		// at least we tried
	}
}

func (ccp *CachedCloudProvider) emit(statser stats.Statser) {
	// regular
	statser.Gauge("cloudprovider.cache_positive", float64(ccp.statsCachePositive), nil)
	statser.Gauge("cloudprovider.cache_negative", float64(ccp.statsCacheNegative), nil)
	statser.Gauge("cloudprovider.cache_refresh_positive", float64(ccp.statsCacheRefreshPositive), nil)
	statser.Gauge("cloudprovider.cache_refresh_negative", float64(ccp.statsCacheRefreshNegative), nil)
}

func (ccp *CachedCloudProvider) doRefresh(t time.Time) {
	var toDelete []gostatsd.IP
	now := t.UnixNano()
	idleNano := ccp.cacheOpts.CacheEvictAfterIdlePeriod.Nanoseconds()

	for ip, holder := range ccp.cache {
		if now-holder.lastAccess() > idleNano {
			// Entry was not used recently, remove it.
			toDelete = append(toDelete, ip)
			if holder.instance == nil {
				ccp.statsCacheNegative--
			} else {
				ccp.statsCachePositive--
			}
		} else if t.After(holder.expires) {
			// Entry needs a refresh.
			ccp.toLookupIPs = append(ccp.toLookupIPs, ip)
		}
	}

	if len(toDelete) > 0 {
		ccp.rw.Lock()
		defer ccp.rw.Unlock()
		for _, ip := range toDelete {
			delete(ccp.cache, ip)
		}
	}
}

func (ccp *CachedCloudProvider) handleInstanceInfo(info gostatsd.InstanceInfo) {
	var ttl time.Duration
	if info.Instance == nil {
		ttl = ccp.cacheOpts.CacheNegativeTTL
	} else {
		ttl = ccp.cacheOpts.CacheTTL
	}
	now := time.Now()
	newHolder := &instanceHolder{
		expires:  now.Add(ttl),
		instance: info.Instance,
	}
	currentHolder := ccp.cache[info.IP]
	if currentHolder == nil {
		// Not in cache, count it
		if info.Instance == nil {
			ccp.statsCacheNegative++
		} else {
			ccp.statsCachePositive++
		}
		newHolder.lastAccessNano = now.UnixNano()
	} else {
		// In cache, don't count it
		newHolder.lastAccessNano = currentHolder.lastAccess()
		if info.Instance == nil {
			// Use the old instance if there was a lookup error.
			newHolder.instance = currentHolder.instance
			ccp.statsCacheRefreshNegative++
		} else {
			if currentHolder.instance == nil && newHolder.instance != nil {
				// An entry has flipped from invalid to valid
				ccp.statsCacheNegative--
				ccp.statsCachePositive++
			}
			ccp.statsCacheRefreshPositive++
		}
	}
	ccp.rw.Lock()
	ccp.cache[info.IP] = newHolder
	ccp.rw.Unlock()
	ccp.toReturnInfo = append(ccp.toReturnInfo, info)
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
