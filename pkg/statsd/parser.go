package statsd

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/statser"

	log "github.com/sirupsen/logrus"
)

// DatagramParser receives datagrams and parses them into Metrics/Events
// For each Metric/Event it calls Handler.HandleMetric/Event()
type DatagramParser struct {
	// Counter fields below must be read/written only using atomic instructions.
	// 64-bit fields must be the first fields in the struct to guarantee proper memory alignment.
	// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	badLines        uint64
	metricsReceived uint64
	eventsReceived  uint64

	ignoreHost bool
	handler    Handler // handler to invoke
	namespace  string  // Namespace to prefix all metrics
	statser    statser.Statser

	in <-chan *Datagram // Input chan of datagrams to parse
}

// NewDatagramParser initialises a new DatagramParser.
func NewDatagramParser(in <-chan *Datagram, ns string, ignoreHost bool, handler Handler, statser statser.Statser) *DatagramParser {
	return &DatagramParser{
		in:         in,
		ignoreHost: ignoreHost,
		handler:    handler,
		namespace:  ns,
		statser:    statser,
	}
}

func (dp *DatagramParser) RunMetrics(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second) // TODO: Make configurable
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dp.statser.Count("metrics_received", float64(atomic.SwapUint64(&dp.metricsReceived, 0)), nil)
			dp.statser.Count("events_received", float64(atomic.SwapUint64(&dp.eventsReceived, 0)), nil)
			dp.statser.Count("bad_lines_seen", float64(atomic.SwapUint64(&dp.badLines, 0)), nil)
		}
	}
}

func (dp *DatagramParser) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case dg := <-dp.in:
			err := dp.handlePacket(ctx, dg.IP, dg.Msg)
			if err != nil {
				if err == context.Canceled || err == context.DeadlineExceeded {
					return
				}
				log.Warnf("Failed to handle packet: %v", err)
			}
		}
	}
}

// handlePacket handles the contents of a datagram and calls Handler.DispatchMetric()
// for each line that successfully parses into a types.Metric and Handler.DispatchEvent() for each event.
func (dp *DatagramParser) handlePacket(ctx context.Context, ip gostatsd.IP, msg []byte) error {
	var numMetrics, numEvents uint16
	var exitError error
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
		metric, event, err := dp.parseLine(line)
		if err != nil {
			// logging as debug to avoid spamming logs when a bad actor sends
			// badly formatted messages
			log.Debugf("Error parsing line %q from %s: %v", line, ip, err)
			atomic.AddUint64(&dp.badLines, 1)
			continue
		}
		if metric != nil {
			numMetrics++
			if dp.ignoreHost {
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
			err = dp.handler.DispatchMetric(ctx, metric)
		} else if event != nil {
			numEvents++
			event.SourceIP = ip // Always keep the source ip for events
			if event.DateHappened == 0 {
				event.DateHappened = time.Now().Unix()
			}
			err = dp.handler.DispatchEvent(ctx, event)
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
	atomic.AddUint64(&dp.metricsReceived, uint64(numMetrics))
	atomic.AddUint64(&dp.eventsReceived, uint64(numEvents))
	return exitError
}

// parseLine with lexer idpl.
func (dp *DatagramParser) parseLine(line []byte) (*gostatsd.Metric, *gostatsd.Event, error) {
	l := lexer{}
	return l.run(line, dp.namespace)
}
