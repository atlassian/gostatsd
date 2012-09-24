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
	flag.StringVar(&metricsAddr, "l", defaultMetricsAddr, "address on which to listen for metrics")
	flag.StringVar(&consoleAddr, "c", defaultConsoleAddr, "address on which to listen for console sessions")
	flag.StringVar(&graphiteAddr, "g", defaultGraphiteAddr, "address of the graphite server")
	flag.DurationVar(&flushInterval, "f", defaultFlushInterval, "how often to flush metrics to the graphite server")
}

func main() {
	flag.Parse()

	// Start the metric aggregator
	graphite, err := statsd.NewGraphiteClient(graphiteAddr)
	if err != nil {
		log.Fatal(err)
	}
	aggregator := statsd.NewMetricAggregator(&graphite, flushInterval)
	go aggregator.Aggregate()

	// Start the metric receiver
	f := func(metric statsd.Metric) {
		aggregator.MetricChan <- metric
	}
	receiver := statsd.MetricReceiver{metricsAddr, statsd.HandlerFunc(f)}
	go receiver.ListenAndReceive()

	// Start the console
	console := statsd.ConsoleServer{consoleAddr, &aggregator}
	go console.ListenAndServe()

	// Listen forever
	select {}
}
