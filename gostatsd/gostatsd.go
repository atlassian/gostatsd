package main

import (
	"flag"
	"github.com/kisielk/gostatsd/statsd"
	"log"
	"time"
)

var (
	metricsAddr   string
	consoleAddr   string
	graphiteAddr  string
	flushInterval time.Duration
)

func init() {
	const (
		defaultMetricsAddr   = ":8125"
		defaultConsoleAddr   = ":8126"
		defaultGraphiteAddr  = "localhost:2003"
		defaultFlushInterval = 10 * time.Second
	)
	flag.StringVar(&metricsAddr, "l", defaultMetricsAddr, "Address on which to listen for metrics")
	flag.StringVar(&consoleAddr, "c", defaultConsoleAddr, "Address on which to listen for console sessions")
	flag.StringVar(&graphiteAddr, "g", defaultGraphiteAddr, "Address of the graphite server")
	flag.DurationVar(&flushInterval, "f", defaultFlushInterval, "How often to flush metrics to the graphite server")
}

func main() {
	flag.Parse()
	err := statsd.ListenAndServe(metricsAddr, consoleAddr, graphiteAddr, flushInterval)
	if err != nil {
		log.Fatal(err)
	}
}
