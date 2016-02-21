package types

import (
	"bytes"
	"fmt"
)

// MetricType is an enumeration of all the possible types of Metric
type MetricType float64

const (
	_                = iota
	ERROR MetricType = 1 << (10 * iota)
	COUNTER
	TIMER
	GAUGE
)

func (m MetricType) String() string {
	switch {
	case m >= GAUGE:
		return "gauge"
	case m >= TIMER:
		return "timer"
	case m >= COUNTER:
		return "counter"
	}
	return "unknown"
}

// Metric represents a single data collected datapoint
type Metric struct {
	Type   MetricType // The type of metric
	Bucket string     // The name of the bucket where the metric belongs
	Value  float64    // The numeric value of the metric
	Tags   []Tag      // The tags for the metric
}

// Tag represents a single tag e.g. foo or foo=1
type Tag struct {
	Key   string
	Value string
}

func (m Tag) String() string {
	if m.Value != "" {
		return fmt.Sprintf("%s:%s", m.Key, m.Value)
	} else {
		return m.Key
	}
}

func (m Metric) String() string {
	return fmt.Sprintf("{%s, %s, %f}", m.Type, m.Bucket, m.Value)
}

// MetricMap is used for storing aggregated Metric values.
// The keys of the map are metric bucket names.
type MetricMap map[string]float64

func (m MetricMap) String() string {
	buf := new(bytes.Buffer)
	for k, v := range m {
		fmt.Fprintf(buf, "%s: %f\n", k, v)
	}
	return buf.String()
}

// MetricListMap is similar to MetricMap but instead of storing a single aggregated
// Metric value it stores a list of all collected values.
type MetricListMap map[string][]float64

func (m MetricListMap) String() string {
	buf := new(bytes.Buffer)
	for k, v := range m {
		buf.Write([]byte(fmt.Sprint(k)))
		for _, v2 := range v {
			fmt.Fprintf(buf, "\t%f\n", k, v2)
		}
	}
	return buf.String()
}
