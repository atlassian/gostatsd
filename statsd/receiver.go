package statsd

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/jtblin/gostatsd/cloudprovider"

	log "github.com/Sirupsen/logrus"
	"github.com/jtblin/gostatsd/types"
)

// DefaultMetricsAddr is the default address on which a MetricReceiver will listen
const (
	defaultMetricsAddr = ":8125"
	maxQueueSize       = 100000      // arbitrary: testing shows it rarely goes above 2k
	packetBufSize      = 1024 * 1024 // 1 MB
	packetSizeUDP      = 1500
)

// Handler interface can be used to handle metrics for a MetricReceiver
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
	Addr       string                  // UDP address on which to listen for metrics
	Cloud      cloudprovider.Interface // Cloud provider interface
	Handler    Handler                 // handler to invoke
	MaxWorkers int                     // Maximum number of workers
	Namespace  string                  // Namespace to prefix all metrics
	Tags       types.Tags              // Tags to add to all metrics
}

type message struct {
	addr net.Addr
	msg  []byte
}

// NewMetricReceiver initialises a new MetricReceiver
func NewMetricReceiver(addr, ns string, maxWorkers int, tags []string, cloud cloudprovider.Interface, handler Handler) *MetricReceiver {
	return &MetricReceiver{
		Addr:       addr,
		Cloud:      cloud,
		Handler:    handler,
		MaxWorkers: maxWorkers,
		Namespace:  ns,
		Tags:       tags,
	}
}

// ListenAndReceive listens on the UDP network address of srv.Addr and then calls
// Receive to handle the incoming datagrams. If Addr is blank then DefaultMetricsAddr is used.
func (mr *MetricReceiver) ListenAndReceive() error {
	addr := mr.Addr
	if addr == "" {
		addr = defaultMetricsAddr
	}
	c, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}

	mq := make(messageQueue, maxQueueSize)
	for i := 0; i < mr.MaxWorkers; i++ {
		go mq.dequeue(mr)
		go mr.receive(c, mq)
	}
	return nil
}

// increment allows counting server stats using default tags
func (mr *MetricReceiver) increment(name string, value int) {
	mr.Handler.HandleMetric(types.NewMetric(internalStatName(name), float64(value), types.COUNTER, mr.Tags))
}

type messageQueue chan message

func (mq messageQueue) enqueue(m message, mr *MetricReceiver) {
	select {
	case mq <- m:
	default:
		mr.increment("dropped_message", 1)
	}
}

func (mq messageQueue) dequeue(mr *MetricReceiver) {
	for m := range mq {
		mr.handleMessage(m.addr, m.msg)
		mr.increment("packets_received", 1)
	}
}

// receive accepts incoming datagrams on c and calls mr.handleMessage() for each message
func (mr *MetricReceiver) receive(c net.PacketConn, mq messageQueue) {
	defer c.Close()

	var buf []byte
	for {
		if len(buf) < packetSizeUDP {
			buf = make([]byte, packetBufSize, packetBufSize)
		}

		nbytes, addr, err := c.ReadFrom(buf)
		if err != nil {
			log.Printf("Error %s", err)
			continue
		}
		msg := buf[:nbytes]
		mq.enqueue(message{addr, msg}, mr)
		buf = buf[nbytes:]
	}
}

// handleMessage handles the contents of a datagram and call r.Handler.HandleMetric()
// for each line that successfully parses in to a types.Metric
func (mr *MetricReceiver) handleMessage(addr net.Addr, msg []byte) {
	numMetrics := 0
	var triedToGetTags bool
	var additionalTags types.Tags
	buf := bytes.NewBuffer(msg)
	for {
		line, readerr := buf.ReadBytes('\n')

		// protocol does not require line to end in \n, if EOF use received line if valid
		if readerr != nil && readerr != io.EOF {
			log.Warnf("Error reading message from %s: %v", addr, readerr)
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
				log.Warnf("Error parsing line %q from %s: %v", line, addr, err)
				mr.increment("bad_lines_seen", 1)
				continue
			}
			if !triedToGetTags {
				triedToGetTags = true
				additionalTags = mr.tryGetTags(addr.String())
			}
			if len(additionalTags) > 0 {
				metric.Tags = append(metric.Tags, additionalTags...)
				log.Debugf("Metric tags: %v", metric.Tags)
			}
			mr.Handler.HandleMetric(metric)
			numMetrics++
		}

		if readerr == io.EOF {
			// if was EOF, finished handling
			mr.increment("metrics_received", numMetrics)
			return
		}
	}
}

func (mr *MetricReceiver) tryGetTags(addr string) types.Tags {
	n := strings.IndexByte(addr, ':')
	if n <= 1 {
		return nil
	}
	hostname := addr[0:n]
	if net.ParseIP(hostname) != nil {
		tags := make(types.Tags, 0, 8)
		if mr.Cloud != nil {
			instance, err := cloudprovider.GetInstance(mr.Cloud, hostname)
			if err != nil {
				log.Warnf("Error retrieving instance details from cloud provider %s: %v", mr.Cloud.ProviderName(), err)
			} else {
				hostname = instance.ID
				tags = append(tags, fmt.Sprintf("region:%s", instance.Region))
				tags = append(tags, instance.Tags...)
			}
		}
		tags = append(tags, fmt.Sprintf("%s:%s", types.StatsdSourceID, hostname))
		return tags
	}
	return nil
}

func (mr *MetricReceiver) parseLine(line []byte) (types.Metric, error) {
	var metric types.Metric
	metric.Tags = append(metric.Tags, mr.Tags...)

	buf := bytes.NewBuffer(line)
	name, err := buf.ReadBytes(':')
	if err != nil {
		return metric, fmt.Errorf("error parsing metric name: %s", err)
	}
	metric.Name = types.NormalizeMetricName(string(name[:len(name)-1]), mr.Namespace)

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
	case "c":
		metric.Type = types.COUNTER
	case "g":
		metric.Type = types.GAUGE
	case "ms":
		metric.Type = types.TIMER
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
			tags, err := mr.parseTags(bits[1])
			if err != nil {
				return metric, fmt.Errorf("error parsing tags: %s", err)
			}
			metric.Tags = append(metric.Tags, tags...)
		}
		if len(bits) > 2 {
			tags, err := mr.parseTags(bits[2])
			if err != nil {
				return metric, fmt.Errorf("error parsing tags: %s", err)
			}
			metric.Tags = append(metric.Tags, tags...)
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
		tags = types.StringToTags(fragment)
	} else {
		err = fmt.Errorf("unknown delimiter: %s", fragment[0:1])
	}
	return
}
