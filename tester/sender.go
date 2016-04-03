package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	"github.com/jtblin/gostatsd/types"

	log "github.com/Sirupsen/logrus"
)

// Metrics store the metrics to send.
type Metrics []*types.Metric

var metrics = &Metrics{
	{
		Name: "statsd.tester.counter",
		Type: types.COUNTER,
	},
	{
		Name: "statsd.tester.gauge",
		Type: types.GAUGE,
	},
	{
		Name: "statsd.tester.timer",
		Type: types.TIMER,
	},
	{
		Name: "statsd.tester.set",
		Type: types.SET,
	},
}

func (s *Server) write(buf *bytes.Buffer) {
	conn, err := net.Dial("udp", s.MetricsAddr)
	if err != nil {
		log.Errorf("error connecting to statsd backend: %s", err)
		return
	}
	defer conn.Close()
	_, err = buf.WriteTo(conn)
	if err != nil {
		log.Errorf("error sending to statsd backend: %s", err)
	}
	atomic.AddInt64(&s.Stats.NumPackets, 1)
}

func (s *Server) writeLine(buf *bytes.Buffer, format, name string, value interface{}) {
	if s.Namespace != "" {
		name = fmt.Sprintf("%s.%s", s.Namespace, name)
	}
	fmt.Fprintf(buf, format, name, value)
	atomic.AddInt64(&s.Stats.NumMetrics, 1)
	// Make sure we don't go over max udp datagram size of 1500
	if buf.Len() > s.MaxPacketSize {
		log.Debugf("Buffer length: %d", buf.Len())
		s.write(buf)
		buf.Reset()
	}
}

func (s *Server) writeLines() *bytes.Buffer {
	buf := new(bytes.Buffer)
	for _, metric := range *metrics {
		switch metric.Type {
		case types.COUNTER:
			value := rand.Float64() * 100
			s.writeLine(buf, "%s:%f|c\n", metric.Name, value)
		case types.TIMER:
			n := rand.Intn(9) + 1
			for i := 0; i < n; i++ {
				value := rand.Float64() * 100
				s.writeLine(buf, "%s:%f|ms\n", metric.Name, value)
			}
		case types.GAUGE:
			value := rand.Float64() * 100
			s.writeLine(buf, "%s:%f|g\n", metric.Name, value)
		case types.SET:
			for i := 0; i < 100; i++ {
				value := rand.Intn(9) + 1
				s.writeLine(buf, "%s:%d|s\n", metric.Name, value)
			}
		}

	}
	return buf
}

func (s *Server) send() {
	var flushTicker *time.Ticker

	for {
		select {
		case <-s.start:
			flushTicker = time.NewTicker(s.FlushInterval)
			atomic.StoreInt64(&s.Stats.NumMetrics, 0)
			atomic.StoreInt64(&s.Stats.NumPackets, 0)
			s.Stats.StartTime = time.Now()
			s.Started = true
			go func() {
				for t := range flushTicker.C {
					log.Debugf("Tick at %v", t)
					rand.Seed(time.Now().Unix())
					buf := s.writeLines()
					if buf.Len() > 0 {
						s.write(buf)
					}
				}
			}()
		case <-s.stop:
			s.Started = false
			flushTicker.Stop()
			s.gatherStats()
		}
	}
}
