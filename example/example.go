package main

import (
	"log"

	"github.com/jtblin/gostatsd/statsd"
	"github.com/jtblin/gostatsd/types"
)

func main() {
	f := func(m types.Metric) {
		log.Printf("%s", m)
	}
	r := statsd.MetricReceiver{":8125", "stats", statsd.HandlerFunc(f)}
	r.ListenAndReceive()
}
