package cloudprovider

import (
	"context"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

const (
	batchDuration = 10 * time.Millisecond
)

type cloudProviderLookupDispatcher struct {
	logger        logrus.FieldLogger
	limiter       *rate.Limiter
	cloudProvider gostatsd.CloudProvider
	ipSource      <-chan gostatsd.IP
	infoSink      chan<- gostatsd.InstanceInfo
}

func (ld *cloudProviderLookupDispatcher) run(ctx context.Context) {
	maxLookupIPs := ld.cloudProvider.MaxInstancesBatch()
	ips := make([]gostatsd.IP, 0, maxLookupIPs)
	var c <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case ip := <-ld.ipSource:
			ips = append(ips, ip)
			if len(ips) >= maxLookupIPs {
				break // enough ips, exit select
			}
			if c == nil {
				c = time.After(batchDuration)
			}
			continue // have some more space-time to collect ips
		case <-c: // time to do the lookup
		}
		c = nil

		if err := ld.limiter.Wait(ctx); err != nil {
			if err != context.Canceled && err != context.DeadlineExceeded {
				// This could be an error caused by context signaling done.
				// Or something nasty but it is very unlikely.
				ld.logger.Warnf("Error from limiter: %v", err)
			}
			return
		}
		ld.doLookup(ctx, ips)
		for i := range ips { // cleanup pointers for GC
			ips[i] = gostatsd.UnknownIP
		}
		ips = ips[:0] // reset slice
	}
}

func (ld *cloudProviderLookupDispatcher) doLookup(ctx context.Context, ips []gostatsd.IP) {
	// instances may contain partial result even if err != nil
	instances, err := ld.cloudProvider.Instance(ctx, ips...)
	if err != nil {
		// Something bad happened, but process what we have still
		ld.logger.Infof("Error retrieving instance details from cloud provider: %v", err)
	}
	for _, ip := range ips {
		res := gostatsd.InstanceInfo{
			IP:       ip,
			Instance: instances[ip],
		}
		select {
		case <-ctx.Done():
			return
		case ld.infoSink <- res:
		}
	}
}
