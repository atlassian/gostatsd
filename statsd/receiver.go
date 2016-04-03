package statsd

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"

	"github.com/jtblin/gostatsd/cloudprovider"
	"github.com/jtblin/gostatsd/types"

	log "github.com/Sirupsen/logrus"
)

// DefaultMetricsAddr is the default address on which a MetricReceiver will listen.
const (
	defaultMetricsAddr = ":8125"
	maxQueueSize       = 100000      // arbitrary: testing shows it rarely goes above 2k
	packetBufSize      = 1024 * 1024 // 1 MB
	packetSizeUDP      = 1500
)

// Handler interface can be used to handle metrics for a MetricReceiver.
type Handler interface {
	HandleMetric(m *types.Metric)
}

// The HandlerFunc type is an adapter to allow the use of ordinary functions as metric handlers.
type HandlerFunc func(*types.Metric)

// HandleMetric calls f(m).
func (f HandlerFunc) HandleMetric(m *types.Metric) {
	f(m)
}

// MetricReceiver receives data on its listening port and converts lines in to Metrics.
// For each types.Metric it calls r.Handler.HandleMetric()
type MetricReceiver struct {
	Addr          string                  // UDP address on which to listen for metrics
	Cloud         cloudprovider.Interface // Cloud provider interface
	Handler       Handler                 // handler to invoke
	MaxReaders    int                     // Maximum number of workers
	MaxMessengers int                     // Maximum number of workers
	Namespace     string                  // Namespace to prefix all metrics
	Tags          types.Tags              // Tags to add to all metrics
}

type message struct {
	addr net.Addr
	msg  []byte
}

// NewMetricReceiver initialises a new MetricReceiver.
func NewMetricReceiver(addr, ns string, maxReaders, maxMessengers int, tags []string, cloud cloudprovider.Interface, handler Handler) *MetricReceiver {
	return &MetricReceiver{
		Addr:          addr,
		Cloud:         cloud,
		Handler:       handler,
		MaxReaders:    maxReaders,
		MaxMessengers: maxMessengers,
		Namespace:     ns,
		Tags:          tags,
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
	for i := 0; i < mr.MaxMessengers; i++ {
		go mq.dequeue(mr)
	}
	for i := 0; i < mr.MaxReaders; i++ {
		go mr.receive(c, mq)
	}
	return nil
}

// increment allows counting server stats using default tags.
func (mr *MetricReceiver) increment(name string, value int) {
	mr.Handler.HandleMetric(types.NewMetric(internalStatName(name), float64(value), types.COUNTER, mr.Tags))
}

type messageQueue chan message

func (mq messageQueue) enqueue(m message, mr *MetricReceiver) {
	mq <- m
}

func (mq messageQueue) dequeue(mr *MetricReceiver) {
	for m := range mq {
		mr.handleMessage(m.addr, m.msg)
		runtime.Gosched()
	}
}

// receive accepts incoming datagrams on c and calls mr.handleMessage() for each message.
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
		mr.increment("packets_received", 1)
		mq.enqueue(message{addr, msg}, mr)
		buf = buf[nbytes:]
		runtime.Gosched()
	}
}

// handleMessage handles the contents of a datagram and call r.Handler.HandleMetric()
// for each line that successfully parses in to a types.Metric.
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

		if len(line) > 1 {
			metric, err := mr.parseLine(line)
			if err != nil {
				// logging as debug to avoid spamming logs when a bad actor sends
				// badly formatted messages
				log.Debugf("Error parsing line %q from %s: %v", line, addr, err)
				mr.increment("bad_lines_seen", 1)
				continue
			}
			if !triedToGetTags {
				triedToGetTags = true
				additionalTags = mr.getAdditionalTags(addr.String())
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

func (mr *MetricReceiver) getAdditionalTags(addr string) types.Tags {
	n := strings.IndexByte(addr, ':')
	if n <= 1 {
		return nil
	}
	hostname := addr[0:n]
	if net.ParseIP(hostname) != nil {
		tags := make(types.Tags, 0, 16)
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

// ParseLine with lexer impl.
func (mr *MetricReceiver) parseLine(line []byte) (*types.Metric, error) {
	llen := len(line)
	if llen == 0 {
		return nil, nil
	}
	metric := &types.Metric{}
	metric.Tags = append(metric.Tags, mr.Tags...)
	l := &lexer{input: line, len: llen, m: metric, namespace: mr.Namespace}
	return l.run()
}
