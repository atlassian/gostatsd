package main

import (
	"log"
	"net"

	"github.com/atlassian/gostatsd/statsd"
	"github.com/atlassian/gostatsd/types"

	"golang.org/x/net/context"
)

func main() {
	r := statsd.NewMetricReceiver("stats", handler{})
	c, err := net.ListenPacket("udp", ":8125")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	log.Fatal(r.Receive(context.TODO(), c))
}

type handler struct{}

func (h handler) DispatchMetric(ctx context.Context, m *types.Metric) error {
	log.Printf("%s", m)
	return nil
}

func (h handler) DispatchEvent(ctx context.Context, e *types.Event) error {
	log.Printf("%s", e)
	return nil
}

func (h handler) WaitForEvents() {
}
