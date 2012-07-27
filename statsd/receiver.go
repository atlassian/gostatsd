package statsd

import (
	"log"
	"net"
)

const DefaultMetricsAddr = ":8125"

type Handler func(Metric)

type MetricReceiver struct {
	Addr    string  // UDP address on which to listen for metrics
	Handler Handler // handler to invoke
}

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

func (r *MetricReceiver) Receive(c net.PacketConn) error {
	defer c.Close()

	msg := make([]byte, 1024)
	for {
		nbytes, _, err := c.ReadFrom(msg)
		if err != nil {
			log.Printf("%s", err)
			continue
		}
		log.Printf("%s", msg[:nbytes])
		go r.handleMessage(msg[:nbytes])
	}
	panic("not reached")
}

func (srv *MetricReceiver) handleMessage(msg []byte) {
	metrics, err := parseMessage(string(msg))
	if err != nil {
		log.Printf("Error parsing metric %s", err)
	}
	for _, metric := range metrics {
		srv.Handler(metric)
	}
}
