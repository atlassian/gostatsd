package gostatsd

import (
	"bytes"
	"fmt"
	"strings"

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
func (mm *MetricMap) Receive(m *Metric) {
	tagsKey := m.FormatTagsKey()

	switch m.Type {
	case COUNTER:
		mm.receiveCounter(m, tagsKey)
	case GAUGE:
		mm.receiveGauge(m, tagsKey)
	case TIMER:
		mm.receiveTimer(m, tagsKey)
	case SET:
		mm.receiveSet(m, tagsKey)
	default:
		logrus.StandardLogger().Errorf("Unknown metric type %s for %s", m.Type, m.Name)
	}
	m.Done()
}

func MergeMaps(mms []*MetricMap) *MetricMap {
	mm := NewMetricMap()
	for _, mmFrom := range mms {
		mm.Merge(mmFrom)
	}
	return mm
}

func (mm *MetricMap) Merge(mmFrom *MetricMap) {
	mmFrom.Counters.Each(mm.MergeCounter)
	mmFrom.Gauges.Each(mm.MergeGauge)
	mmFrom.Sets.Each(mm.MergeSet)
	mmFrom.Timers.Each(mm.MergeTimer)
}

func (mm *MetricMap) MergeCounter(metricName string, tagsKey string, counterFrom Counter) {
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
}

func (mm *MetricMap) MergeGauge(metricName string, tagsKey string, gaugeFrom Gauge) {
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
}

func (mm *MetricMap) MergeSet(metricName string, tagsKey string, setFrom Set) {
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
}

func (mm *MetricMap) MergeTimer(metricName string, tagsKey string, timerFrom Timer) {
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
}

func (mm *MetricMap) IsEmpty() bool {
	return len(mm.Counters)+len(mm.Timers)+len(mm.Sets)+len(mm.Gauges) == 0
}

// Split will split a MetricMap up in to multiple MetricMaps, where each one contains metrics only for its buckets.
func (mm *MetricMap) Split(count int) []*MetricMap {
	maps := make([]*MetricMap, count)
	for i := 0; i < count; i++ {
		maps[i] = NewMetricMap()
	}

	mm.Counters.Each(func(metricName string, tagsKey string, c Counter) {
		mmSplit := maps[Bucket(metricName, c.Source, count)]
		if v, ok := mmSplit.Counters[metricName]; ok {
			v[tagsKey] = c
		} else {
			mmSplit.Counters[metricName] = map[string]Counter{tagsKey: c}
		}
	})
	mm.Gauges.Each(func(metricName string, tagsKey string, g Gauge) {
		mmSplit := maps[Bucket(metricName, g.Source, count)]
		if v, ok := mmSplit.Gauges[metricName]; ok {
			v[tagsKey] = g
		} else {
			mmSplit.Gauges[metricName] = map[string]Gauge{tagsKey: g}
		}
	})
	mm.Timers.Each(func(metricName string, tagsKey string, t Timer) {
		mmSplit := maps[Bucket(metricName, t.Source, count)]
		if v, ok := mmSplit.Timers[metricName]; ok {
			v[tagsKey] = t
		} else {
			mmSplit.Timers[metricName] = map[string]Timer{tagsKey: t}
		}
	})
	mm.Sets.Each(func(metricName string, tagsKey string, s Set) {
		mmSplit := maps[Bucket(metricName, s.Source, count)]
		if v, ok := mmSplit.Sets[metricName]; ok {
			v[tagsKey] = s
		} else {
			mmSplit.Sets[metricName] = map[string]Set{tagsKey: s}
		}
	})

	return maps
}

func tagsMatch(tagNames []string, tagsKey string) string {
	res := make([]string, 0)
	for _, tv := range strings.Split(tagsKey, ",") {
		for _, tagName := range tagNames {
			if tagName == "" {
				break
			}
			if strings.HasPrefix(tv, tagName) {
				res = append(res, tv)
				break
			}
		}
	}
	return strings.Join(res, ",")
}

func (mm *MetricMap) SplitByTags(tagNames []string) map[string]*MetricMap {
	maps := make(map[string]*MetricMap)
	if len(tagNames) == 0 {
		maps[""] = mm
		return maps
	}
	mm.Counters.Each(func(metricName string, tagsKey string, c Counter) {
		key := tagsMatch(tagNames, tagsKey)
		if _, ok := maps[key]; !ok {
			maps[key] = NewMetricMap()
		}
		mmSplit := maps[key]
		if v, ok := mmSplit.Counters[metricName]; ok {
			v[tagsKey] = c
		} else {
			mmSplit.Counters[metricName] = map[string]Counter{tagsKey: c}
		}
	})

	mm.Gauges.Each(func(metricName string, tagsKey string, g Gauge) {
		key := tagsMatch(tagNames, tagsKey)
		if _, ok := maps[key]; !ok {
			maps[key] = NewMetricMap()
		}
		mmSplit := maps[key]
		if v, ok := mmSplit.Gauges[metricName]; ok {
			v[tagsKey] = g
		} else {
			mmSplit.Gauges[metricName] = map[string]Gauge{tagsKey: g}
		}
	})

	mm.Timers.Each(func(metricName string, tagsKey string, t Timer) {
		key := tagsMatch(tagNames, tagsKey)
		if _, ok := maps[key]; !ok {
			maps[key] = NewMetricMap()
		}
		mmSplit := maps[key]
		if v, ok := mmSplit.Timers[metricName]; ok {
			v[tagsKey] = t
		} else {
			mmSplit.Timers[metricName] = map[string]Timer{tagsKey: t}
		}
	})

	mm.Sets.Each(func(metricName string, tagsKey string, s Set) {
		key := tagsMatch(tagNames, tagsKey)
		if _, ok := maps[key]; !ok {
			maps[key] = NewMetricMap()
		}
		mmSplit := maps[key]
		if v, ok := mmSplit.Sets[metricName]; ok {
			v[tagsKey] = s
		} else {
			mmSplit.Sets[metricName] = map[string]Set{tagsKey: s}
		}
	})

	return maps
}

func (mm *MetricMap) receiveCounter(m *Metric, tagsKey string) {
	value := int64(m.Value / m.Rate)
	v, ok := mm.Counters[m.Name]
	if ok {
		c, ok := v[tagsKey]
		if ok {
			c.Value += value
			if m.Timestamp > c.Timestamp {
				c.Timestamp = m.Timestamp
			}
		} else {
			c = NewCounter(m.Timestamp, value, m.Source, m.Tags)
		}
		v[tagsKey] = c
	} else {
		mm.Counters[m.Name] = map[string]Counter{
			tagsKey: NewCounter(m.Timestamp, value, m.Source, m.Tags),
		}
	}
}

func (mm *MetricMap) receiveGauge(m *Metric, tagsKey string) {
	v, ok := mm.Gauges[m.Name]
	if ok {
		g, ok := v[tagsKey]
		if ok {
			if m.Timestamp > g.Timestamp {
				g.Value = m.Value
				g.Timestamp = m.Timestamp
			}
		} else {
			g = NewGauge(m.Timestamp, m.Value, m.Source, m.Tags)
		}
		v[tagsKey] = g
	} else {
		mm.Gauges[m.Name] = map[string]Gauge{
			tagsKey: NewGauge(m.Timestamp, m.Value, m.Source, m.Tags),
		}
	}
}

func (mm *MetricMap) receiveTimer(m *Metric, tagsKey string) {
	v, ok := mm.Timers[m.Name]
	if ok {
		t, ok := v[tagsKey]
		if ok {
			t.Values = append(t.Values, m.Value)
			if m.Timestamp > t.Timestamp {
				t.Timestamp = m.Timestamp
			}
			t.SampledCount += 1.0 / m.Rate
		} else {
			t = NewTimer(m.Timestamp, []float64{m.Value}, m.Source, m.Tags)
			t.SampledCount = 1.0 / m.Rate
		}
		v[tagsKey] = t
	} else {
		t := NewTimer(m.Timestamp, []float64{m.Value}, m.Source, m.Tags)
		t.SampledCount = 1.0 / m.Rate

		mm.Timers[m.Name] = map[string]Timer{
			tagsKey: t,
		}
	}
}

func (mm *MetricMap) receiveSet(m *Metric, tagsKey string) {
	v, ok := mm.Sets[m.Name]
	if ok {
		s, ok := v[tagsKey]
		if ok {
			s.Values[m.StringValue] = struct{}{}
			if m.Timestamp > s.Timestamp {
				s.Timestamp = m.Timestamp
			}
		} else {
			s = NewSet(m.Timestamp, map[string]struct{}{m.StringValue: {}}, m.Source, m.Tags)
		}
		v[tagsKey] = s
	} else {
		mm.Sets[m.Name] = map[string]Set{
			tagsKey: NewSet(m.Timestamp, map[string]struct{}{m.StringValue: {}}, m.Source, m.Tags),
		}
	}
}

func (mm *MetricMap) String() string {
	buf := new(bytes.Buffer)
	mm.Counters.Each(func(k, tags string, counter Counter) {
		_, _ = fmt.Fprintf(buf, "stats.counter.%s: %d tags=%s\n", k, counter.Value, tags)
	})
	mm.Timers.Each(func(k, tags string, timer Timer) {
		for _, value := range timer.Values {
			_, _ = fmt.Fprintf(buf, "stats.timer.%s: %f tags=%s\n", k, value, tags)
		}
	})
	mm.Gauges.Each(func(k, tags string, gauge Gauge) {
		_, _ = fmt.Fprintf(buf, "stats.gauge.%s: %f tags=%s\n", k, gauge.Value, tags)
	})
	mm.Sets.Each(func(k, tags string, set Set) {
		_, _ = fmt.Fprintf(buf, "stats.set.%s: %d tags=%s\n", k, len(set.Values), tags)
	})
	return buf.String()
}

// AsMetrics will synthesize Metrics from the MetricMap and return them as a slice
func (mm *MetricMap) AsMetrics() []*Metric {
	var metrics []*Metric

	mm.Counters.Each(func(metricName string, tagsKey string, c Counter) {
		m := &Metric{
			Name:      metricName,
			Type:      COUNTER,
			Value:     float64(c.Value),
			Rate:      1,
			Tags:      c.Tags.Copy(),
			TagsKey:   tagsKey,
			Timestamp: c.Timestamp,
			Source:    c.Source,
		}
		metrics = append(metrics, m)
	})

	mm.Gauges.Each(func(metricName string, tagsKey string, g Gauge) {
		m := &Metric{
			Name:      metricName,
			Type:      GAUGE,
			Value:     g.Value,
			Rate:      1,
			Tags:      g.Tags.Copy(),
			TagsKey:   tagsKey,
			Timestamp: g.Timestamp,
			Source:    g.Source,
		}
		metrics = append(metrics, m)
	})

	mm.Timers.Each(func(metricName string, tagsKey string, t Timer) {
		// Compensate for t.SampledCount so the final handler will multiply it back out.  This whole thing will
		// disappear once the backend aggregator is refactored (issue #210)
		rate := float64(len(t.Values)) / t.SampledCount
		for _, value := range t.Values {
			m := &Metric{
				Name:      metricName,
				Type:      TIMER,
				Value:     value,
				Rate:      rate,
				Tags:      t.Tags.Copy(),
				TagsKey:   tagsKey,
				Timestamp: t.Timestamp,
				Source:    t.Source,
			}
			metrics = append(metrics, m)
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
				Timestamp:   s.Timestamp,
				Source:      s.Source,
			}
			metrics = append(metrics, m)
		}
	})

	return metrics
}
