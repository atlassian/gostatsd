package statsd

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

// DefaultMetricsAddr is the default address on which a MetricReceiver will listen
const DefaultMetricsAddr = ":8125"

// Objects implementing the Handler interface can be used to handle metrics for a MetricReceiver
type Handler interface {
	HandleMetric(m Metric)
}

// The HandlerFunc type is an adapter to allow the use of ordinary functions as metric handlers
type HandlerFunc func(Metric)

// HandleMetric calls f(m)
func (f HandlerFunc) HandleMetric(m Metric) {
	f(m)
}

// MetricReceiver receives data on its listening port and converts lines in to Metrics.
// For each Metric it calls r.Handler.HandleMetric()
type MetricReceiver struct {
	Addr    string  // UDP address on which to listen for metrics
	Handler Handler // handler to invoke
}

// ListenAndReceive listens on the UDP network address of srv.Addr and then calls
// Receive to handle the incoming datagrams. If Addr is blank then DefaultMetricsAddr is used.
func (r *MetricReceiver) ListenAndReceive() error {
	addr := r.Addr
	if addr == "" {
		addr = DefaultMetricsAddr
	}
	c, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}
	return r.Receive(c)
}

// Receive accepts incoming datagrams on c and calls r.Handler.HandleMetric() for each line in the
// datagram that successfully parses in to a Metric
func (r *MetricReceiver) Receive(c net.PacketConn) error {
	defer c.Close()

	msg := make([]byte, 1024)
	for {
		nbytes, _, err := c.ReadFrom(msg)
		if err != nil {
			log.Printf("%s", err)
			continue
		}
		go r.handleMessage(msg[:nbytes])
	}
	panic("not reached")
}

// handleMessage handles the contents of a datagram and attempts to parse a Metric from each line
func (srv *MetricReceiver) handleMessage(msg []byte) {
	metrics, err := parseMessage(string(msg))
	if err != nil {
		log.Printf("Error parsing metric %s", err)
	}
	for _, metric := range metrics {
		srv.Handler.HandleMetric(metric)
	}
}

// parseMessage parses a message string string in to a list of metrics
func parseMessage(msg string) ([]Metric, error) {
	metricList := []Metric{}

	segments := strings.Split(strings.TrimSpace(msg), ":")
	if len(segments) < 1 {
		return metricList, fmt.Errorf("ill-formatted message: %s", msg)
	}

	bucket := segments[0]
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
