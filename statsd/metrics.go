package statsd

import (
	"bytes"
	"fmt"
	"regexp"
)

// Regular expressions used for bucket name normalization
var (
	regSpaces  = regexp.MustCompile("\\s+")
	regSlashes = regexp.MustCompile("\\/")
	regInvalid = regexp.MustCompile("[^a-zA-Z_\\-0-9\\.]")
)

type MetricType float64

// Enumeration, see http://golang.org/doc/effective_go.html#constants
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

type Metric struct {
	Type   MetricType
	Bucket string
	Value  float64
}

func (m Metric) String() string {
	return fmt.Sprintf("{%s, %s, %f}", m.Type, m.Bucket, m.Value)
}

type MetricMap map[string]float64

func (m MetricMap) String() string {
	buf := new(bytes.Buffer)
	for k, v := range m {
		fmt.Fprintf(buf, "%s: %f\n", k, v)
	}
	return buf.String()
}

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
