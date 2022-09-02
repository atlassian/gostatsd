package stats

import (
	"sync/atomic"

	"github.com/atlassian/gostatsd"
)

// repeatCount is how many times to send the gauge once it changes.  22 is used because it will cover at
// least 2 flush intervals on an aggregation server with a 10x longer interval than a forwarding server.
const repeatCount = 22

// ChangeGauge is a simpler wrapper to send a gauge for a rare event, multiple times.  It is primarily intended for
// things that won't change in the majority of cases (such as failure to parse a statsd line), and things that will
// be bursty (ie, transient http message failure).  It isn't suitable for things which are constantly changing such
// as number of metrics or http requests.
type ChangeGauge struct {
	// Cur is the last value expected to be sent.  If this is changed, SendIfChanged will send the value for
	// 22 flush intervals in an attempt to retry any failures to send it.  Cur is assumed to be atomic by
	// ChangeGauge.SendIfChanged, however if the owner has more specific knowledge, it can be written to
	// with non-atomic operations.
	Cur uint64 // atomic

	prev    uint64
	pending uint64 // number of times to re-send
}

func (cg *ChangeGauge) SendIfChanged(statser Statser, metricName string, tags gostatsd.Tags) {
	v := atomic.LoadUint64(&cg.Cur)
	if v != cg.prev {
		cg.prev = v
		cg.pending = repeatCount
	}
	if cg.pending > 0 {
		cg.pending--
		statser.Gauge(metricName, float64(v), tags)
	}
}
