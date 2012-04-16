package main

import (
	"fmt"
	"log"
	"math"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)


var flushInterval time.Duration
var graphiteServer string
var percentThresholds []float64

func init() {
	flushInterval = 10 * time.Second
	graphiteServer = "localhost:1234"
	percentThresholds = []float64{90.0}
}

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

func round(v float64) float64 {
	return math.Floor(v + 0.5)
}

func average(vals []float64) float64 {
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func thresholdStats(vals []float64, threshold float64) (mean, upper float64) {
	if count := len(vals); count > 1 {
		idx := int(round(((100 - threshold) / 100) * float64(count)))
		thresholdCount := count - idx
		thresholdValues := vals[:thresholdCount]

		mean = average(thresholdValues)
		upper = thresholdValues[len(thresholdValues) - 1]
	} else {
		mean = vals[0]
		upper = vals[0]
	}
	return mean, upper
}

func flushMetrics(counters MetricMap, gauges MetricMap, timers MetricListMap, flushInterval time.Duration) {
	conn, err := net.Dial("tcp", graphiteServer)
	if err != nil {
		log.Printf("Could not contact Graphite server")
		return
	}
	defer conn.Close()

	numStats := 0
	now := time.Now().Unix()

	for k, v := range counters {
		perSecond := v / flushInterval.Seconds()
		fmt.Fprintf(conn, "stats.%s %f %d\n", k, perSecond, now)
		fmt.Fprintf(conn, "stats_counts.%s %f %d\n", k, v, now)
		numStats += 1
	}

	for k, v := range gauges {
		fmt.Fprintf(conn, "stats.gauges.%s %f %d\n", k, v, now)
		numStats += 1
	}

	for k, v := range timers {
		if count := len(v); count > 0 {
			sort.Float64s(v)
			min := v[0]
			max := v[count-1]

			fmt.Fprintf(conn, "stats.timers.%s.lower %f %d\n", k, min, now)
			fmt.Fprintf(conn, "stats.timers.%s.upper %f %d\n", k, max, now)
			fmt.Fprintf(conn, "stats.timers.%s.count %d %d\n", k, count, now)

			for _, threshold := range percentThresholds {
				mean, upper := thresholdStats(v, threshold)
				thresholdName := strconv.FormatFloat(threshold, 'f', 1, 64)
				fmt.Fprintf(conn, "stats.timers.%s.mean_%s %f %d\n", k, thresholdName, mean, now)
				fmt.Fprintf(conn, "stats.timers.%s.upper_%s %f %d\n", k, thresholdName, upper, now)
			}
			numStats += 1
		}
	}
	fmt.Fprintf(conn, "statsd.numStats %d %d\n", numStats, now)

}

func metricAggregator(metrics chan Metric) {
	var counters = make(MetricMap)
	var gauges = make(MetricMap)
	var timers = make(MetricListMap)

	flushTimer := time.NewTimer(flushInterval)

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
		case <-flushTimer.C:
			go flushMetrics(counters, gauges, timers, flushInterval)

			// Reset counters
			new_counters := make(MetricMap)
			for k := range counters {
				new_counters[k] = 0
			}
			counters = new_counters

			// Reset timers
			new_timers := make(MetricListMap)
			for k := range timers {
				new_timers[k] = []float64{}
			}
			timers = new_timers

			// Keep values of gauges
			new_gauges := make(MetricMap)
			for k, v := range gauges {
				new_gauges[k] = v
			}
			gauges = new_gauges

			flushTimer = time.NewTimer(flushInterval)
		}
	}
}

func normalizeBucketName(name string) string {
	spaces, _ := regexp.Compile("\\s+")
	slashes, _ := regexp.Compile("\\/")
	invalid, _ := regexp.Compile("[^a-zA-Z_\\-0-9\\.]")
	return invalid.ReplaceAllString(slashes.ReplaceAllString(spaces.ReplaceAllString(name, "_"), "-"), "")
}

func parseMessage(msg string) ([]Metric, error) {
	metricList := []Metric{}

	segments := strings.Split(strings.TrimSpace(msg), ":")
	if len(segments) < 1 {
		return metricList, fmt.Errorf("ill-formatted message: %s", msg)
	}

	bucket := normalizeBucketName(segments[0])
	var values []string
	if len(segments) == 1 {
		values = []string{"1"}
	} else {
		values = segments[1:]
	}

	for _, value := range values {
		fields := strings.Split(value, "|")

		metricValue, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return metricList, fmt.Errorf("%s: bad metric value \"%s\"", bucket, fields[0])
		}

		var metricTypeString string
		if len(fields) == 1 {
			metricTypeString = "c"
		} else {
			metricTypeString = fields[1]
		}

		var metricType MetricType
		switch metricTypeString {
		case "ms":
			// Timer
			metricType = TIMER
		case "g":
			// Gauge
			metricType = GAUGE
		default:
			// Counter, allows skipping of |c suffix
			metricType = COUNTER

			var rate float64
			if len(fields) == 3 {
				var err error
				rate, err = strconv.ParseFloat(fields[2][1:], 64)
				if err != nil {
					return metricList, fmt.Errorf("%s: bad rate %s", fields[2])
				}
			} else {
				rate = 1
			}
			metricValue = metricValue / rate
		}

		metric := Metric{metricType, bucket, metricValue}
		metricList = append(metricList, metric)
	}

	return metricList, nil
}

func handleMessage(metric_chan chan Metric, msg string) {
	metrics, err := parseMessage(msg)
	if err != nil {
		log.Printf("Error parsing metric %s", err)
	} else {
		for _, metric := range metrics {
			log.Printf("%s", metric)
			metric_chan <- metric
		}
	}
}
