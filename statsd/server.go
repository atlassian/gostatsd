package statsd

import (
	"bytes"
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

// Regular expressions used for bucket name normalization
var (
	regSpaces  = regexp.MustCompile("\\s+")
	regSlashes = regexp.MustCompile("\\/")
	regInvalid = regexp.MustCompile("[^a-zA-Z_\\-0-9\\.]")
)

var flushInterval time.Duration
var graphiteServer string
var percentThresholds []float64

func init() {
	flushInterval = 10 * time.Second
	percentThresholds = []float64{90.0}
}

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

type MetricAggregatorStats struct {
	BadLines          int
	LastMessage       time.Time
	GraphiteLastFlush time.Time
	GraphiteLastError time.Time
}

func (m Metric) String() string {
	return fmt.Sprintf("{%s, %s, %f}", m.Type, m.Bucket, m.Value)
}

type MetricMap map[string]float64
type MetricListMap map[string][]float64


type GraphiteClient struct {
	conn *net.Conn
}

func NewGraphiteClient(addr string) (client GraphiteClient, err error) {
	conn, err := net.Dial("tcp", addr)
	client = GraphiteClient{&conn}
	return
}

func (client *GraphiteClient) SendMetrics(metrics MetricMap) (err error) {
	buf := new(bytes.Buffer)
	now := time.Now().Unix()
	for k, v := range metrics {
		fmt.Fprintf(buf, "%s %f %d\n", k, v, now)
	}
	_, err = buf.WriteTo(*client.conn)
	if err != nil {
		return err
	}
	return nil
}

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

func (m MetricMap) String() string {
	buf := new(bytes.Buffer)
	for k, v := range m {
		fmt.Fprintf(buf, "%s: %f\n", k, v)
	}
	return buf.String()
}

type ConsoleRequest struct {
	Command    string
	ResultChan chan string
}

type ConsoleSession struct {
	RequestChan chan string
	ResultChan  chan string
}

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
		upper = thresholdValues[len(thresholdValues)-1]
	} else {
		mean = vals[0]
		upper = vals[0]
	}
	return mean, upper
}

func aggregateMetrics(counters MetricMap, gauges MetricMap, timers MetricListMap, flushInterval time.Duration) (metrics MetricMap) {
	metrics = make(MetricMap)
	numStats := 0

	for k, v := range counters {
		perSecond := v / flushInterval.Seconds()
		metrics["stats." + k] = perSecond
		metrics["stats_counts." + k] = v
		numStats += 1
	}

	for k, v := range gauges {
		metrics["stats.gauges." + k] = v
		numStats += 1
	}

	for k, v := range timers {
		if count := len(v); count > 0 {
			sort.Float64s(v)
			min := v[0]
			max := v[count-1]

			metrics["stats.timers." + k + ".lower"] = min
			metrics["stats.timers." + k + ".upper"] = max
			metrics["stats.timers." + k + ".count"] = float64(count)

			for _, threshold := range percentThresholds {
				mean, upper := thresholdStats(v, threshold)
				thresholdName := strconv.FormatFloat(threshold, 'f', 1, 64)
				metrics["stats.timers." + k + "mean_" + thresholdName] = mean
				metrics["stats.timers." + k + "upper_" + thresholdName] = upper
			}
			numStats += 1
		}
	}
	metrics["statsd.numStats"] = float64(numStats)
	return metrics
}

func metricAggregator(graphiteAddr string, metricChan chan Metric, consoleChan chan ConsoleRequest) (err error) {
	graphite, err := NewGraphiteClient(graphiteAddr)
	if err != nil {
		return
	}
	stats := new(MetricAggregatorStats)
	counters := make(MetricMap)
	gauges := make(MetricMap)
	timers := make(MetricListMap)

	flushTimer := time.NewTimer(flushInterval)
	flushChan := make(chan error)

	log.Printf("Started aggregator")

	for {
		select {
		case metric := <-metricChan: // Incoming metrics
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
			case ERROR:
				stats.BadLines += 1
			}
			stats.LastMessage = time.Now()
		case <-flushTimer.C: // Time to flush to graphite
			go graphite.SendMetrics(aggregateMetrics(counters, gauges, timers, flushInterval))

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
		case flushResult := <-flushChan:
			if flushResult != nil {
				log.Printf("Sending metrics to Graphite failed: %s", flushResult)
				stats.GraphiteLastError = time.Now()
			} else {
				stats.GraphiteLastFlush = time.Now()
			}
		case consoleRequest := <-consoleChan:
			var result string
			switch parts := strings.Split(strings.TrimSpace(consoleRequest.Command), " "); parts[0] {
			case "help":
				result = "Commands: stats, counters, timers, gauges, delcounters, deltimers, delgauges, quit\n"
			case "stats":
				result = fmt.Sprintf(
					"Invalid messages received: %d\n"+
						"Last message received: %s\n"+
						"Last flush to Graphite: %s\n"+
						"Last error from Graphite: %s\n",
					stats.BadLines, stats.LastMessage, stats.GraphiteLastFlush, stats.GraphiteLastError)
			case "counters":
				result = fmt.Sprint(counters)
			case "timers":
				result = fmt.Sprint(timers)
			case "gauges":
				result = fmt.Sprint(gauges)
			case "delcounters":
				for _, k := range parts[1:] {
					delete(counters, k)
				}
			case "deltimers":
				for _, k := range parts[1:] {
					delete(timers, k)
				}
			case "delgauges":
				for _, k := range parts[1:] {
					delete(gauges, k)
				}
			case "quit":
				result = "quit"
			default:
				result = fmt.Sprintf("unknown command: %s\n", parts[0])
			}
			consoleRequest.ResultChan <- result
		}
	}

	return
}


// Normalize a bucket name by replacing or translating invalid characters
func normalizeBucketName(name string) string {
	nospaces := regSpaces.ReplaceAllString(name, "_")
	noslashes := regSlashes.ReplaceAllString(nospaces, "-")
	return regInvalid.ReplaceAllString(noslashes, "")
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

func handleMessage(metricChan chan Metric, msg string) {
	metrics, err := parseMessage(msg)
	if err != nil {
		log.Printf("Error parsing metric %s", err)
	} else {
		for _, metric := range metrics {
			metricChan <- metric
		}
	}
}

func metricListener(addr string, metricChan chan Metric) {
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		log.Fatal(err)
		return
	}
	msg := make([]byte, 1024)
	for {
		nbytes, _, err := conn.ReadFrom(msg)
		if err != nil {
			log.Printf("%s", err)
			continue
		}
		go handleMessage(metricChan, string(msg[:nbytes]))
	}
}

func consoleClient(conn net.Conn, consoleChan chan ConsoleRequest) {
	defer conn.Close()

	command := make([]byte, 1024)
	resultChan := make(chan string)

	for {
		nbytes, err := conn.Read(command)
		if err != nil {
			// Connection has likely closed
			return
		}
		consoleChan <- ConsoleRequest{string(command[:nbytes]), resultChan}
		result := <-resultChan
		if result == "quit" {
			return
		}
		conn.Write([]byte(result))
	}
}

func consoleServer(addr string, consoleChan chan ConsoleRequest) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
		return
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("%s", err)
			continue
		}
		go consoleClient(conn, consoleChan)
	}
}

func ListenAndServe(metricAddr string, consoleAddr string, graphiteAddr string) error {
	var metricChan = make(chan Metric)
	var consoleChan = make(chan ConsoleRequest)
	go metricListener(metricAddr, metricChan)
	go metricAggregator(graphiteAddr, metricChan, consoleChan)
	go consoleServer(consoleAddr, consoleChan)
	// Run forever
	select {}
	return nil
}
