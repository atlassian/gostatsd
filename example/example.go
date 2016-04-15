package main

import (
	"log"
	"net"

	"github.com/atlassian/gostatsd/statsd"
	"github.com/atlassian/gostatsd/types"

	"golang.org/x/net/context"
)

func main() {
	f := func(ctx context.Context, m *types.Metric) error {
		log.Printf("%s", m)
		return nil
	}
	r := statsd.NewMetricReceiver("stats", nil, nil, statsd.HandlerFunc(f))
	c, err := net.ListenPacket("udp", ":8125")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	r.Receive(context.TODO(), c)
}
