package statsd

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd"

	log "github.com/Sirupsen/logrus"
)

// ip packet size is stored in two bytes and that is how big in theory the packet can be.
// In practice it is highly unlikely but still possible to get packets bigger than usual MTU of 1500.
const packetSizeUDP = 0xffff

// MetricReceiver receives data on its PacketConn and converts lines into Metrics.
// For each types.Metric it calls Handler.HandleMetric()
type MetricReceiver struct {
	// Counter fields below must be read/written only using atomic instructions.
	// 64-bit fields must be the first fields in the struct to guarantee proper memory alignment.
	// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	lastPacket      int64 // When last packet was received. Unix timestamp in nsec.
	badLines        uint64
	packetsReceived uint64
	metricsReceived uint64
	eventsReceived  uint64
	ignoreHost      bool
	handler         Handler // handler to invoke
	namespace       string  // Namespace to prefix all metrics
}

// NewMetricReceiver initialises a new MetricReceiver.
func NewMetricReceiver(ns string, ignoreHost bool, handler Handler) *MetricReceiver {
	return &MetricReceiver{
		ignoreHost: ignoreHost,
		handler:    handler,
		namespace:  ns,
	}
}

// GetStats returns current MetricReceiver stats. Safe for concurrent use.
func (mr *MetricReceiver) GetStats() ReceiverStats {
	return ReceiverStats{
		LastPacket:      time.Unix(0, atomic.LoadInt64(&mr.lastPacket)),
		BadLines:        atomic.LoadUint64(&mr.badLines),
		PacketsReceived: atomic.LoadUint64(&mr.packetsReceived),
		MetricsReceived: atomic.LoadUint64(&mr.metricsReceived),
		EventsReceived:  atomic.LoadUint64(&mr.eventsReceived),
	}
}

// Receive accepts incoming datagrams on c, parses them and calls Handler.DispatchMetric() for each metric
// and Handler.DispatchEvent() for each event.
func (mr *MetricReceiver) Receive(ctx context.Context, done gostatsd.Done, c net.PacketConn) {
	defer done()
	buf := make([]byte, packetSizeUDP)
	for {
		// This will error out when the socket is closed.
		nbytes, addr, err := c.ReadFrom(buf)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			log.Warnf("Error reading from socket: %v", err)
			continue
		}
		// TODO consider updating counter for every N-th iteration to reduce contention
		atomic.AddUint64(&mr.packetsReceived, 1)
		atomic.StoreInt64(&mr.lastPacket, time.Now().UnixNano())
		if err := mr.handlePacket(ctx, addr, buf[:nbytes]); err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				return
			}
			log.Warnf("Failed to handle packet: %v", err)
		}
	}
}

// handlePacket handles the contents of a datagram and calls Handler.DispatchMetric()
// for each line that successfully parses into a types.Metric and Handler.DispatchEvent() for each event.
func (mr *MetricReceiver) handlePacket(ctx context.Context, addr net.Addr, msg []byte) error {
	var numMetrics, numEvents uint16
	var exitError error
	ip := getIP(addr)
	for {
		idx := bytes.IndexByte(msg, '\n')
		var line []byte
		// protocol does not require line to end in \n
		if idx == -1 { // \n not found
			if len(msg) == 0 {
				break
			}
			line = msg
			msg = nil
		} else { // usual case
			line = msg[:idx]
			msg = msg[idx+1:]
		}
		metric, event, err := mr.parseLine(line)
		if err != nil {
			// logging as debug to avoid spamming logs when a bad actor sends
			// badly formatted messages
			log.Debugf("Error parsing line %q from %s: %v", line, ip, err)
			atomic.AddUint64(&mr.badLines, 1)
			continue
		}
		if metric != nil {
			numMetrics++
			if mr.ignoreHost {
				for idx, tag := range metric.Tags {
					if strings.HasPrefix(tag, "host:") {
						metric.Hostname = tag[5:]
						if len(metric.Tags) > 1 {
							metric.Tags = append(metric.Tags[:idx], metric.Tags[idx+1:]...)
						} else {
							metric.Tags = nil
						}
						break
					}
				}
			} else {
				metric.SourceIP = ip
			}
			err = mr.handler.DispatchMetric(ctx, metric)
		} else if event != nil {
			numEvents++
			event.SourceIP = ip // Always keep the source ip for events
			if event.DateHappened == 0 {
				event.DateHappened = time.Now().Unix()
			}
			err = mr.handler.DispatchEvent(ctx, event)
		} else {
			// Should never happen.
			log.Panic("Both event and metric are nil")
		}
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				exitError = err
				break
			}
			log.Warnf("Error dispatching metric/event %q from %s: %v", line, ip, err)
		}
	}
	atomic.AddUint64(&mr.metricsReceived, uint64(numMetrics))
	atomic.AddUint64(&mr.eventsReceived, uint64(numEvents))
	return exitError
}

// parseLine with lexer impl.
func (mr *MetricReceiver) parseLine(line []byte) (*gostatsd.Metric, *gostatsd.Event, error) {
	l := lexer{}
	return l.run(line, mr.namespace)
}

func getIP(addr net.Addr) gostatsd.IP {
	if a, ok := addr.(*net.UDPAddr); ok {
		return gostatsd.IP(a.IP.String())
	}
	log.Errorf("Cannot get source address %q of type %T", addr, addr)
	return gostatsd.UnknownIP
}
