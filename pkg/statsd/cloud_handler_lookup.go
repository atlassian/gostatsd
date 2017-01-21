package statsd

import (
	"context"
	"sync"

	"github.com/atlassian/gostatsd"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/time/rate"
)

type lookupDispatcher struct {
	wg            sync.WaitGroup
	limiter       *rate.Limiter
	cloud         gostatsd.CloudProvider // Cloud provider interface
	lookupResults chan<- *lookupResult
}

func (ld *lookupDispatcher) run(ctx context.Context, toLookup <-chan gostatsd.IP) {
	ld.wg.Add(1)
	defer log.Info("Cloud lookup dispatcher stopped")
	defer ld.wg.Done()

	for ip := range toLookup {
		if err := ld.limiter.Wait(ctx); err != nil {
			if err != context.Canceled && err != context.DeadlineExceeded {
				// This could be an error caused by context signaling done. Or something nasty but it is very unlikely.
				log.Warnf("Error from limiter: %v", err)
			}
			return
		}
		ld.wg.Add(1)
		go ld.doLookup(ctx, ip)
	}
}

func (ld *lookupDispatcher) doLookup(ctx context.Context, ip gostatsd.IP) {
	defer ld.wg.Done()

	instance, err := ld.cloud.Instance(ctx, ip)
	res := &lookupResult{
		err:      err,
		ip:       ip,
		instance: instance,
	}
	select {
	case <-ctx.Done():
	case ld.lookupResults <- res:
	}
}

func (ld *lookupDispatcher) join() {
	ld.wg.Wait() // Wait for all in-flight lookups to finish
}
