package statsd

import (
	"bytes"
	"fmt"
	"io"
	"net"
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

// MetricReceiver receives data on its listening port and converts lines in to Metrics.
// For each types.Metric it calls r.Handler.HandleMetric()
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
// datagram that successfully parses in to a types.Metric
func (r *MetricReceiver) Receive(c net.PacketConn) error {
	defer c.Close()

	msg := make([]byte, UDPPacketSize)
	for {
		nbytes, addr, err := c.ReadFrom(msg)
		if err != nil {
			log.Printf("%s", err)
			continue
		}
		buf := make([]byte, nbytes)
		copy(buf, msg[:nbytes])
		go r.handleMessage(addr, buf)
	}
	panic("not reached")
}

// handleMessage handles the contents of a datagram and attempts to parse a types.Metric from each line
func (srv *MetricReceiver) handleMessage(addr net.Addr, msg []byte) {
	buf := bytes.NewBuffer(msg)
	for {
		line, readerr := buf.ReadBytes('\n')

		// protocol does not require line to end in \n, if EOF use received line if valid
		if readerr != nil && readerr != io.EOF {
			log.Printf("error reading message from %s: %s", addr, readerr)
			return
		} else if readerr != io.EOF {
			// remove newline, only if not EOF
			if len(line) > 0 {
				line = line[:len(line)-1]
			}
		}

		// Only process lines with more than one character
		if len(line) > 1 {
			metric, err := parseLine(line)
			if err != nil {
				log.Printf("error parsing line %q from %s: %s", line, addr, err)
				continue
			}
			go srv.Handler.HandleMetric(metric)
		}

		if readerr == io.EOF {
			// if was EOF, finished handling
			return
		}
	}
}

func parseLine(line []byte) (types.Metric, error) {
	var metric types.Metric

	buf := bytes.NewBuffer(line)
	bucket, err := buf.ReadBytes(':')
	if err != nil {
		return metric, fmt.Errorf("error parsing metric name: %s", err)
	}
	metric.Bucket = string(bucket[:len(bucket)-1])

	value, err := buf.ReadBytes('|')
	if err != nil {
		return metric, fmt.Errorf("error parsing metric value: %s", err)
	}
	metric.Value, err = strconv.ParseFloat(string(value[:len(value)-1]), 64)
	if err != nil {
		return metric, fmt.Errorf("error converting metric value: %s", err)
	}

	endLine := string(buf.Bytes())
	if err != nil && err != io.EOF {
		return metric, fmt.Errorf("error parsing metric type: %s", err)
	}

	bits := strings.Split(endLine, "|")

	metricType := bits[0]

	switch metricType[:] {
	case "ms":
		// Timer
		metric.Type = types.TIMER
	case "g":
		// Gauge
		metric.Type = types.GAUGE
	case "c":
		metric.Type = types.COUNTER
	default:
		err = fmt.Errorf("invalid metric type: %q", metricType)
		return metric, err
	}

	sampleRate := 1.0
	if len(bits) > 1 {
		if strings.HasPrefix(bits[1], "@") {
			sampleRate, err = strconv.ParseFloat(bits[1][1:], 64)
			if err != nil {
				return metric, fmt.Errorf("error converting sample rate: %s", err)
			}
		} else {
			metric.Tags, err = parseTags(bits[1])
			if err != nil {
				return metric, fmt.Errorf("error parsing tags: %s", err)
			}
		}
		if len(bits) > 2 {
			metric.Tags, err = parseTags(bits[2])
			if err != nil {
				return metric, fmt.Errorf("error parsing tags: %s", err)
			}
		}
	}

	if metric.Type == types.COUNTER {
		metric.Value = metric.Value / sampleRate
	}

	log.Debugf("tags: %+v", metric.Tags)
	log.Debugf("metric: %+v", metric)
	return metric, nil
}

func parseTags(fragment string) (tags []types.Tag, err error) {
	if strings.HasPrefix(fragment, "#") {
		fragment = fragment[1:]
		bits := strings.Split(fragment, ",")
		for _, bit := range bits {
			s := strings.Split(bit, ":")
			tag := types.Tag{Key: s[0]}
			if len(s) > 1 {
				tag.Value = s[1]
			}
			tags = append(tags, tag)
		}
	} else {
		err = fmt.Errorf("unknown delimiter: %s", fragment[0:1])
	}
	return
}
