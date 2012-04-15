package main

import (
	"net"
	"fmt"
	"log"
	"time"
	"strings"
	"regexp"
	"strconv"
)

const FLUSH_INTERVAL = 1000

type MetricType int

// Enumeration, see http://golang.org/doc/effective_go.html#constants
const (
	_ = iota
	COUNTER MetricType = 1 << (10 * iota)
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
	Type MetricType
	Bucket string
	Value float64
}

func (m Metric) String() string {
	return fmt.Sprintf("{%s, %s, %f}", m.Type, m.Bucket, m.Value)
}

func main () {
	var metrics = make(chan Metric)
	go metricListener(metrics)
	go metricAggregator(metrics)
	// Run forever
	select {}
}

func metricListener(metrics chan Metric) {
	conn, err := net.ListenPacket("udp", ":8125")
	if err != nil {
		// Do something about it
		log.Fatal(err)
		return
	}
	msg := make([]byte, 1024)
	for {
		nbytes, _, err := conn.ReadFrom(msg)
		if err != nil {
			log.Fatal(err)
			continue
		}
		go handleMessage(metrics, string(msg[:nbytes]))
	}
}

type MetricMap map[string]float64
type MetricListMap map[string][]float64

func flushMetrics(counters MetricMap, gauges MetricMap, timers MetricListMap) {

}

func metricAggregator(metrics chan Metric) {
	var counters = make(MetricMap)
	var gauges = make(MetricMap)
	var timers = make(MetricListMap)

	flush_timer := time.NewTimer(FLUSH_INTERVAL)

	log.Printf("Started aggregator")

	for {
		select {
		case metric := <-metrics:
			log.Printf("Got %s", metric)
			switch metric.Type {
			case COUNTER:
				v, ok := counters[metric.Bucket]
				if ok {
					counters[metric.Bucket] = v + metric.Value
				} else {
					counters[metric.Bucket] = metric.Value
				}
			case GAUGE:
				gauges[metric.Bucket] = metric.Value
			case TIMER:
				v, ok := timers[metric.Bucket]
				if ok {
					v = append(v, metric.Value)
					timers[metric.Bucket] = v
				} else {
					timers[metric.Bucket] = []float64{metric.Value}
				}
			}
		case <-flush_timer.C:
			go flushMetrics(counters, gauges, timers)

			// Reset counters
			for k := range counters {
				counters[k] = 0
			}
			// Reset timers
			for k := range timers {
				timers[k] = []float64{}
			}
			// Note: gauges are not reset

			flush_timer = time.NewTimer(FLUSH_INTERVAL)
		}
	}
}

func normalizeBucketName(name string) string {
	spaces, _ := regexp.Compile("\\s+")
	slashes, _ := regexp.Compile("\\/")
	invalid, _ := regexp.Compile("[^a-zA-Z_\\-0-9\\.]")
	return invalid.ReplaceAllString(slashes.ReplaceAllString(spaces.ReplaceAllString(name, "_"), "-"), "")
}

func handleMessage(metrics chan Metric, msg string) {
	segments := strings.Split(strings.TrimSpace(msg), ":")
	if len(segments) < 1 {
		log.Printf("Received ill-formatted message: %s", msg)
		return
	}

	bucket := normalizeBucketName(segments[0])
	var values []string
	if len(segments) == 1 {
		values = []string{"1|c"}
	} else {
		values = segments[1:]
	}

	for _, value := range values {
		//sampleRate := 1
		fields := strings.Split(value, "|")

		if len(fields) == 1 {
			log.Printf("Bad value for %s: %s", bucket, value)
			return
		}

		metric_value, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			log.Printf("Bad metric value for %s: %s", bucket, fields[0])
			return
		}

		var metric_type MetricType
		switch fields[1] {
		case "ms":
			// Timer
			metric_type = TIMER
		case "g":
			// Gauge
			metric_type = GAUGE
		case "c":
			// Counter
			metric_type = COUNTER

			var rate float64
			if len(fields) == 3 {
				var err error
				rate, err = strconv.ParseFloat(fields[2][1:], 64)
				if err != nil {
					log.Printf("Could not parse rate from %s", fields[2])
					return
				}
			} else {
				rate = 1
			}
			metric_value = metric_value / rate
		default:
			log.Printf("Unknown metric type: %s", metric_type)
			return
		}

		metric := Metric{metric_type, bucket, metric_value}
		log.Printf("%s", metric)
		metrics <- metric
	}
}
