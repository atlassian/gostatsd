package statsd

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/jtblin/gostatsd/cloudprovider"

	log "github.com/Sirupsen/logrus"
	"github.com/jtblin/gostatsd/types"
)

// DefaultMetricsAddr is the default address on which a MetricReceiver will listen
const (
	defaultMetricsAddr = ":8125"
	packetSizeUDP      = 1500
	maxQueueSize       = 1000
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
	BufferPool sync.Pool               // Buffer pool
	Cloud      cloudprovider.Interface // Cloud provider interface
	Handler    Handler                 // handler to invoke
	MaxWorkers int                     // Maximum number of workers
	Namespace  string                  // Namespace to prefix all metrics
	Tags       []string                // Tags to add to all metrics
}

type message struct {
	addr   net.Addr
	msg    []byte
	length int
}

// NewMetricReceiver initialises a new MetricReceiver
func NewMetricReceiver(addr, ns string, maxWorkers int, tags []string, cloud cloudprovider.Interface, handler Handler) *MetricReceiver {
	return &MetricReceiver{
		Addr: addr,
		BufferPool: sync.Pool{
			New: func() interface{} { return make([]byte, packetSizeUDP) },
		},
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

func (mr *MetricReceiver) countInternalStats(name string, value int) {
	mr.Handler.HandleMetric(types.Metric{
		Type:  types.COUNTER,
		Name:  fmt.Sprintf("statsd.%s", name),
		Tags:  mr.Tags,
		Value: float64(value),
	})
}

type messageQueue chan message

func (mq messageQueue) enqueue(m message) {
	mq <- m
}

func (mq messageQueue) dequeue(mr *MetricReceiver) {
	for m := range mq {
		mr.handleMessage(m.addr, m.msg[0:m.length])
		mr.BufferPool.Put(m.msg)
		mr.countInternalStats("packets_received", 1)
	}
}

// receive accepts incoming datagrams on c and calls mr.handleMessage() for each message
func (mr *MetricReceiver) receive(c net.PacketConn, mq messageQueue) error {
	defer c.Close()

	for {
		msg := mr.BufferPool.Get().([]byte)
		nbytes, addr, err := c.ReadFrom(msg[0:])
		if err != nil {
			log.Errorf("%s", err)
			continue
		}
		mq.enqueue(message{addr, msg, nbytes})
	}
}

// handleMessage handles the contents of a datagram and call r.Handler.HandleMetric()
// for each line that successfully parses in to a types.Metric
func (mr *MetricReceiver) handleMessage(addr net.Addr, msg []byte) {
	numMetrics := 0
	buf := bytes.NewBuffer(msg)
	for {
		line, readerr := buf.ReadBytes('\n')

		// protocol does not require line to end in \n, if EOF use received line if valid
		if readerr != nil && readerr != io.EOF {
			log.Errorf("Error reading message from %s: %v", addr, readerr)
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
				log.Errorf("Error parsing line %q from %s: %v", line, addr, err)
				mr.countInternalStats("bad_lines_seen", 1)
				continue
			}
			source := strings.Split(addr.String(), ":")
			hostname := source[0]
			if net.ParseIP(hostname) != nil {
				if mr.Cloud != nil {
					instance, err := cloudprovider.GetInstance(mr.Cloud, hostname)
					if err != nil {
						log.Errorf("Error retrieving instance details from cloud provider %s: %v", mr.Cloud.ProviderName(), err)
					} else {
						hostname = instance.ID
						metric.Tags = append(metric.Tags, instance.Tags...)
						metric.Tags = append(metric.Tags, fmt.Sprintf("%s:%s", "region", instance.Region))
					}
				}
				metric.Tags = append(metric.Tags, fmt.Sprintf("%s:%s", types.StatsdSourceID, hostname))
				log.Debugf("Metric tags: %v", metric.Tags)
			}
			mr.Handler.HandleMetric(metric)
			numMetrics++
		}

		if readerr == io.EOF {
			// if was EOF, finished handling
			mr.countInternalStats("metrics_received", numMetrics)
			return
		}
	}
}

func (mr *MetricReceiver) parseLine(line []byte) (types.Metric, error) {
	var metric types.Metric
	metric.Tags = mr.Tags

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
		tags = strings.Split(fragment, ",")
	} else {
		err = fmt.Errorf("unknown delimiter: %s", fragment[0:1])
	}
	return
}
