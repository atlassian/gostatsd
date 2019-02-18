package gostatsd

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// MetricMap is used for storing aggregated or consolidated Metric values.
// The keys of each map are metric names.
type MetricMap struct {
	Counters Counters
	Timers   Timers
	Gauges   Gauges
	Sets     Sets
}

func NewMetricMap() *MetricMap {
	return &MetricMap{
		Counters: Counters{},
		Timers:   Timers{},
		Gauges:   Gauges{},
		Sets:     Sets{},
	}
}

// Receive adds a single Metric to the MetricMap, and releases the Metric.
func (mm *MetricMap) Receive(m *Metric, now time.Time) {
	tagsKey := m.TagsKey
	nowNano := Nanotime(now.UnixNano())

	switch m.Type {
	case COUNTER:
		mm.receiveCounter(m, tagsKey, nowNano)
	case GAUGE:
		mm.receiveGauge(m, tagsKey, nowNano)
	case TIMER:
		mm.receiveTimer(m, tagsKey, nowNano)
	case SET:
		mm.receiveSet(m, tagsKey, nowNano)
	default:
		logrus.StandardLogger().Errorf("Unknown metric type %s for %s", m.Type, m.Name)
	}
	m.Done()
}

func (mm *MetricMap) Merge(mmFrom *MetricMap) {
	mmFrom.Counters.Each(func(metricName string, tagsKey string, counterFrom Counter) {
		v, ok := mm.Counters[metricName]
		if ok {
			counterInto, ok := v[tagsKey]
			if ok {
				if counterInto.Timestamp < counterFrom.Timestamp {
					counterInto.Timestamp = counterFrom.Timestamp
				}
				counterInto.Value += counterFrom.Value
			} else {
				counterInto = counterFrom
			}
			v[tagsKey] = counterInto
		} else {
			mm.Counters[metricName] = map[string]Counter{
				tagsKey: counterFrom,
			}
		}
	})

	mmFrom.Gauges.Each(func(metricName string, tagsKey string, gaugeFrom Gauge) {
		v, ok := mm.Gauges[metricName]
		if ok {
			gaugeInto, ok := v[tagsKey]
			if ok {
				if gaugeInto.Timestamp < gaugeFrom.Timestamp {
					gaugeInto.Timestamp = gaugeFrom.Timestamp
					gaugeInto.Value = gaugeFrom.Value
				}
			} else {
				gaugeInto = gaugeFrom
			}
			v[tagsKey] = gaugeInto
		} else {
			mm.Gauges[metricName] = map[string]Gauge{
				tagsKey: gaugeFrom,
			}
		}
	})

	mmFrom.Timers.Each(func(metricName string, tagsKey string, timerFrom Timer) {
		v, ok := mm.Timers[metricName]
		if ok {
			timerInto, ok := v[tagsKey]
			if ok {
				if timerInto.Timestamp < timerFrom.Timestamp {
					timerInto.Timestamp = timerFrom.Timestamp
				}
				timerInto.Values = append(timerInto.Values, timerFrom.Values...)
				timerInto.SampledCount += timerFrom.SampledCount
			} else {
				timerInto = timerFrom
			}
			v[tagsKey] = timerInto
		} else {
			mm.Timers[metricName] = map[string]Timer{
				tagsKey: timerFrom,
			}
		}
	})
	mmFrom.Sets.Each(func(metricName string, tagsKey string, setFrom Set) {
		v, ok := mm.Sets[metricName]
		if ok {
			setInto, ok := v[tagsKey]
			if ok {
				if setInto.Timestamp < setFrom.Timestamp {
					setInto.Timestamp = setFrom.Timestamp
				}
				for setValue := range setFrom.Values {
					setInto.Values[setValue] = struct{}{}
				}
			} else {
				setInto = setFrom
			}
			v[tagsKey] = setInto
		} else {
			mm.Sets[metricName] = map[string]Set{
				tagsKey: setFrom,
			}
		}
	})
}

func (mm *MetricMap) receiveCounter(m *Metric, tagsKey string, now Nanotime) {
	value := int64(m.Value / m.Rate)
	v, ok := mm.Counters[m.Name]
	if ok {
		c, ok := v[tagsKey]
		if ok {
			c.Value += value
			c.Timestamp = now
		} else {
			c = NewCounter(now, value, m.Hostname, m.Tags)
		}
		v[tagsKey] = c
	} else {
		mm.Counters[m.Name] = map[string]Counter{
			tagsKey: NewCounter(now, value, m.Hostname, m.Tags),
		}
	}
}

func (mm *MetricMap) receiveGauge(m *Metric, tagsKey string, now Nanotime) {
	// TODO: handle +/-
	v, ok := mm.Gauges[m.Name]
	if ok {
		g, ok := v[tagsKey]
		if ok {
			g.Value = m.Value
			g.Timestamp = now
		} else {
			g = NewGauge(now, m.Value, m.Hostname, m.Tags)
		}
		v[tagsKey] = g
	} else {
		mm.Gauges[m.Name] = map[string]Gauge{
			tagsKey: NewGauge(now, m.Value, m.Hostname, m.Tags),
		}
	}
}

func (mm *MetricMap) receiveTimer(m *Metric, tagsKey string, now Nanotime) {
	v, ok := mm.Timers[m.Name]
	if ok {
		t, ok := v[tagsKey]
		if ok {
			t.Values = append(t.Values, m.Value)
			t.Timestamp = now
			t.SampledCount += 1.0 / m.Rate
		} else {
			t = NewTimer(now, []float64{m.Value}, m.Hostname, m.Tags)
			t.SampledCount = 1.0 / m.Rate
		}
		v[tagsKey] = t
	} else {
		t := NewTimer(now, []float64{m.Value}, m.Hostname, m.Tags)
		t.SampledCount = 1.0 / m.Rate

		mm.Timers[m.Name] = map[string]Timer{
			tagsKey: t,
		}
	}
}

func (mm *MetricMap) receiveSet(m *Metric, tagsKey string, now Nanotime) {
	v, ok := mm.Sets[m.Name]
	if ok {
		s, ok := v[tagsKey]
		if ok {
			s.Values[m.StringValue] = struct{}{}
			s.Timestamp = now
		} else {
			s = NewSet(now, map[string]struct{}{m.StringValue: {}}, m.Hostname, m.Tags)
		}
		v[tagsKey] = s
	} else {
		mm.Sets[m.Name] = map[string]Set{
			tagsKey: NewSet(now, map[string]struct{}{m.StringValue: {}}, m.Hostname, m.Tags),
		}
	}
}

func (mm *MetricMap) String() string {
	buf := new(bytes.Buffer)
	mm.Counters.Each(func(k, tags string, counter Counter) {
		fmt.Fprintf(buf, "stats.counter.%s: %d tags=%s\n", k, counter.Value, tags)
	})
	mm.Timers.Each(func(k, tags string, timer Timer) {
		for _, value := range timer.Values {
			fmt.Fprintf(buf, "stats.timer.%s: %f tags=%s\n", k, value, tags)
		}
	})
	mm.Gauges.Each(func(k, tags string, gauge Gauge) {
		fmt.Fprintf(buf, "stats.gauge.%s: %f tags=%s\n", k, gauge.Value, tags)
	})
	mm.Sets.Each(func(k, tags string, set Set) {
		fmt.Fprintf(buf, "stats.set.%s: %d tags=%s\n", k, len(set.Values), tags)
	})
	return buf.String()
}

// DispatchMetrics will synthesize Metrics from the MetricMap and push them to the supplied PipelineHandler
func (mm *MetricMap) DispatchMetrics(ctx context.Context, handler RawMetricHandler) {
	mm.Counters.Each(func(metricName string, tagsKey string, c Counter) {
		m := &Metric{
			Name:     metricName,
			Type:     COUNTER,
			Value:    float64(c.Value),
			Rate:     1,
			Tags:     c.Tags.Copy(),
			TagsKey:  tagsKey,
			Hostname: c.Hostname,
		}
		handler.DispatchMetric(ctx, m)
	})

	mm.Gauges.Each(func(metricName string, tagsKey string, g Gauge) {
		m := &Metric{
			Name:     metricName,
			Type:     GAUGE,
			Value:    g.Value,
			Rate:     1,
			Tags:     g.Tags.Copy(),
			TagsKey:  tagsKey,
			Hostname: g.Hostname,
		}
		handler.DispatchMetric(ctx, m)
	})

	mm.Timers.Each(func(metricName string, tagsKey string, t Timer) {
		// Compensate for t.SampledCount so the final handler will multiply it back out.  This whole thing will
		// disappear once the backend aggregator is refactored (issue #210)
		rate := float64(len(t.Values)) / t.SampledCount
		for _, value := range t.Values {
			m := &Metric{
				Name:     metricName,
				Type:     TIMER,
				Value:    value,
				Rate:     rate,
				Tags:     t.Tags.Copy(),
				TagsKey:  tagsKey,
				Hostname: t.Hostname,
			}
			handler.DispatchMetric(ctx, m)
		}
	})

	mm.Sets.Each(func(metricName string, tagsKey string, s Set) {
		for value := range s.Values {
			m := &Metric{
				Name:        metricName,
				Type:        SET,
				StringValue: value,
				Rate:        1.0,
				Tags:        s.Tags.Copy(),
				TagsKey:     tagsKey,
				Hostname:    s.Hostname,
			}
			handler.DispatchMetric(ctx, m)
		}
	})
}
