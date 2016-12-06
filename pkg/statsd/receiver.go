package statsd

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd"

	log "github.com/Sirupsen/logrus"
)

// ip packet size is stored in two bytes and that is how big in theory the packet can be.
// In practice it is highly unlikely but still possible to get packets bigger than usual MTU of 1500.
const packetSizeUDP = 0xffff

// Receiver receives data on its PacketConn and converts lines into Metrics.
// For each types.Metric it calls Handler.HandleMetric()
type Receiver interface {
	Receive(context.Context, net.PacketConn) error
	GetStats() ReceiverStats
}

// ReceiverStats holds statistics for a Receiver.
type ReceiverStats struct {
	LastPacket      time.Time
	BadLines        uint64
	PacketsReceived uint64
	MetricsReceived uint64
	EventsReceived  uint64
}

type metricReceiver struct {
	// Counter fields below must be read/written only using atomic instructions.
	// 64-bit fields must be the first fields in the struct to guarantee proper memory alignment.
	// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	lastPacket      int64 // When last packet was received. Unix timestamp in nsec.
	badLines        uint64
	packetsReceived uint64
	metricsReceived uint64
	eventsReceived  uint64
	handler         Handler // handler to invoke
	namespace       string  // Namespace to prefix all metrics
}

// NewMetricReceiver initialises a new Receiver.
func NewMetricReceiver(ns string, handler Handler) Receiver {
	return &metricReceiver{
		handler:   handler,
		namespace: ns,
	}
}

// GetStats returns current Receiver stats. Safe for concurrent use.
func (mr *metricReceiver) GetStats() ReceiverStats {
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
func (mr *metricReceiver) Receive(ctx context.Context, c net.PacketConn) error {
	buf := make([]byte, packetSizeUDP)
	for {
		// This will error out when the socket is closed.
		nbytes, addr, err := c.ReadFrom(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && !netErr.Temporary() {
				select {
				case <-ctx.Done():
				default:
					return fmt.Errorf("non-temporary error reading from socket: %v", err)
				}
				return nil
			}
			log.Warnf("Error reading from socket: %v", err)
			continue
		}
		// TODO consider updating counter for every N-th iteration to reduce contention
		atomic.AddUint64(&mr.packetsReceived, 1)
		atomic.StoreInt64(&mr.lastPacket, time.Now().UnixNano())
		if err := mr.handlePacket(ctx, addr, buf[:nbytes]); err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				return err
			}
			log.Warnf("Failed to handle packet: %v", err)
		}
	}
}

// handlePacket handles the contents of a datagram and calls Handler.DispatchMetric()
// for each line that successfully parses into a types.Metric and Handler.DispatchEvent() for each event.
func (mr *metricReceiver) handlePacket(ctx context.Context, addr net.Addr, msg []byte) error {
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
			metric.SourceIP = ip
			err = mr.handler.DispatchMetric(ctx, metric)
		} else if event != nil {
			numEvents++
			event.SourceIP = ip
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
func (mr *metricReceiver) parseLine(line []byte) (*gostatsd.Metric, *gostatsd.Event, error) {
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
