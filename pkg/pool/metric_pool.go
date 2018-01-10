package pool

import (
	"sync"

	"github.com/atlassian/gostatsd"
)

// MetricPool is a strongly typed wrapper around a sync.Pool for *gostatsd.Metric, it provides
// two main benefits: 1) metrics are very short lived and we create a lot of them, 2) reuse
// of the tags buffer
type MetricPool struct {
	p             sync.Pool
	estimatedTags int
}

// NewMetricPool returns a new metric pool.
func NewMetricPool(estimatedTags int) *MetricPool {
	return &MetricPool{
		p: sync.Pool{
			New: func() interface{} {
				return &gostatsd.Metric{}
			},
		},
		estimatedTags: estimatedTags,
	}
}

// Get returns a *gostatsd.Metric suitable for holding a metric.  The DoneFunc should be called
// when the metric is no longer required.  It must not be called earlier, and the Tags field may
// be reused.
func (mp *MetricPool) Get() *gostatsd.Metric {
	m := mp.p.Get().(*gostatsd.Metric)
	if m.DoneFunc != nil { // it was re-used, and the data needs cleaning
		m.Name = ""
		m.Value = 0
		m.Tags = m.Tags[:0]
		m.StringValue = ""
		m.Hostname = ""
		m.SourceIP = ""
	} else {
		m.DoneFunc = func() {
			mp.p.Put(m)
		}
		if mp.estimatedTags != 0 {
			m.Tags = make(gostatsd.Tags, 0, mp.estimatedTags)
		}
	}
	return m
}
