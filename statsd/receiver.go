package statsd

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jtblin/gostatsd/types"
)

// DefaultMetricsAddr is the default address on which a MetricReceiver will listen
const (
	DefaultMetricsAddr = ":8125"
	UDPPacketSize      = 1500
)

// Regular expressions used for metric name normalization
var (
	regSpaces  = regexp.MustCompile("\\s+")
	regSlashes = regexp.MustCompile("\\/")
	regInvalid = regexp.MustCompile("[^a-zA-Z_\\-0-9\\.]")
)

// Objects implementing the Handler interface can be used to handle metrics for a MetricReceiver
type Handler interface {
	HandleMetric(m types.Metric)
}

// The HandlerFunc type is an adapter to allow the use of ordinary functions as metric handlers
type HandlerFunc func(types.Metric)

// HandleMetric calls f(m)
func (f HandlerFunc) HandleMetric(m types.Metric) {
	f(m)
}

// normalizeMetricName cleans up a metric name by replacing or translating invalid characters
func (mr *MetricReceiver) normalizeMetricName(name string) string {
	nospaces := regSpaces.ReplaceAllString(name, "_")
	noslashes := regSlashes.ReplaceAllString(nospaces, "-")
	metricName := regInvalid.ReplaceAllString(noslashes, "")
	if mr.Namespace != "" {
		metricName = fmt.Sprintf("%s.%s", mr.Namespace, metricName)
	}
	return metricName
}

// MetricReceiver receives data on its listening port and converts lines in to Metrics.
// For each types.Metric it calls r.Handler.HandleMetric()
type MetricReceiver struct {
	Addr      string  // UDP address on which to listen for metrics
	Namespace string  // Namespace to prefix all metrics
	Handler   Handler // handler to invoke
}

// ListenAndReceive listens on the UDP network address of srv.Addr and then calls
// Receive to handle the incoming datagrams. If Addr is blank then DefaultMetricsAddr is used.
func (mr *MetricReceiver) ListenAndReceive() error {
	addr := mr.Addr
	if addr == "" {
		addr = DefaultMetricsAddr
	}
	c, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}
	return mr.Receive(c)
}

func (mr *MetricReceiver) countInternalStats(name string, value int) {
	mr.Handler.HandleMetric(types.Metric{
		Type:  types.COUNTER,
		Name:  fmt.Sprintf("statsd.%s", name),
		Value: float64(value),
	})
}

// Receive accepts incoming datagrams on c and calls r.Handler.HandleMetric() for each line in the
// datagram that successfully parses in to a types.Metric
func (mr *MetricReceiver) Receive(c net.PacketConn) error {
	defer c.Close()

	msg := make([]byte, UDPPacketSize)
	for {
		nbytes, addr, err := c.ReadFrom(msg)
		if err != nil {
			log.Errorf("%s", err)
			continue
		}
		buf := make([]byte, nbytes)
		copy(buf, msg[:nbytes])
		go mr.handleMessage(addr, buf)
		go mr.countInternalStats("packets_received", 1)
	}
	panic("not reached")
}

// handleMessage handles the contents of a datagram and attempts to parse a types.Metric from each line
func (mr *MetricReceiver) handleMessage(addr net.Addr, msg []byte) {
	numMetrics := 0
	buf := bytes.NewBuffer(msg)
	for {
		line, readerr := buf.ReadBytes('\n')

		// protocol does not require line to end in \n, if EOF use received line if valid
		if readerr != nil && readerr != io.EOF {
			log.Errorf("error reading message from %s: %s", addr, readerr)
			return
		} else if readerr != io.EOF {
			// remove newline, only if not EOF
			if len(line) > 0 {
				line = line[:len(line)-1]
			}
		}

		// Only process lines with more than one character
		if len(line) > 1 {
			metric, err := mr.parseLine(line)
			if err != nil {
				log.Errorf("error parsing line %q from %s: %s", line, addr, err)
				go mr.countInternalStats("bad_lines_seen", 1)
				continue
			}
			source := strings.Split(addr.String(), ":")
			if net.ParseIP(source[0]) != nil {
				metric.Source = source[0]
			}
			go mr.Handler.HandleMetric(metric)
			numMetrics++
		}

		if readerr == io.EOF {
			// if was EOF, finished handling
			go mr.countInternalStats("metrics_received", numMetrics)
			return
		}
	}
}

func (mr *MetricReceiver) parseLine(line []byte) (types.Metric, error) {
	var metric types.Metric

	buf := bytes.NewBuffer(line)
	name, err := buf.ReadBytes(':')
	if err != nil {
		return metric, fmt.Errorf("error parsing metric name: %s", err)
	}
	metric.Name = mr.normalizeMetricName(string(name[:len(name)-1]))

	value, err := buf.ReadBytes('|')
	if err != nil {
		return metric, fmt.Errorf("error parsing metric value: %s", err)
	}
	metricValue := string(value[:len(value)-1])

	endLine := string(buf.Bytes())
	if err != nil && err != io.EOF {
		return metric, fmt.Errorf("error parsing metric type: %s", err)
	}

	bits := strings.Split(endLine, "|")

	metricType := bits[0]

	switch metricType[:] {
	case "ms":
		metric.Type = types.TIMER
	case "g":
		metric.Type = types.GAUGE
	case "c":
		metric.Type = types.COUNTER
	case "s":
		metric.Type = types.SET
	default:
		err = fmt.Errorf("invalid metric type: %q", metricType)
		return metric, err
	}

	if metric.Type == types.SET {
		metric.StringValue = metricValue
	} else {
		metric.Value, err = strconv.ParseFloat(metricValue, 64)
		if err != nil {
			return metric, fmt.Errorf("error converting metric value: %s", err)
		}
	}

	sampleRate := 1.0
	if len(bits) > 1 {
		if strings.HasPrefix(bits[1], "@") {
			sampleRate, err = strconv.ParseFloat(bits[1][1:], 64)
			if err != nil {
				return metric, fmt.Errorf("error converting sample rate: %s", err)
			}
		} else {
			metric.Tags, err = mr.parseTags(bits[1])
			if err != nil {
				return metric, fmt.Errorf("error parsing tags: %s", err)
			}
		}
		if len(bits) > 2 {
			metric.Tags, err = mr.parseTags(bits[2])
			if err != nil {
				return metric, fmt.Errorf("error parsing tags: %s", err)
			}
		}
	}

	if metric.Type == types.COUNTER {
		metric.Value = metric.Value / sampleRate
	}

	log.Debugf("metric: %+v", metric)
	return metric, nil
}

func (mr *MetricReceiver) parseTags(fragment string) (tags types.Tags, err error) {
	if strings.HasPrefix(fragment, "#") {
		fragment = fragment[1:]
		tags.Items = strings.Split(fragment, ",")
	} else {
		err = fmt.Errorf("unknown delimiter: %s", fragment[0:1])
	}
	return
}
