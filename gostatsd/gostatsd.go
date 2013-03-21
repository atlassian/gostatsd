package main

import (
	"flag"
	"github.com/kisielk/gostatsd/statsd"
	"log"
	"time"
)

const (
	defaultMetricsAddr   = ":8125"
	defaultConsoleAddr   = ":8126"
	defaultGraphiteAddr  = "localhost:2003"
	defaultFlushInterval = 10 * time.Second
)

func main() {
	metricsAddr := flag.String("l", defaultMetricsAddr, "address on which to listen for metrics")
	graphiteAddr := flag.String("g", defaultGraphiteAddr, "address of the graphite server")
	flushInterval := flag.Duration("f", defaultFlushInterval, "how often to flush metrics to the graphite server")
	webConsoleAddr := flag.String("web", "", "if set, use as the address of the web-based console")
	consoleAddr := flag.String("console", "", "if set, use as the address of the telnet-based console ")
	flag.Parse()

	// Start the metric aggregator
	graphite, err := statsd.NewGraphiteClient(*graphiteAddr)
	if err != nil {
		log.Fatal(err)
	}
	aggregator := statsd.NewMetricAggregator(&graphite, *flushInterval)
	go aggregator.Aggregate()

	// Start the metric receiver
	f := func(metric statsd.Metric) {
		aggregator.MetricChan <- metric
	}
	receiver := statsd.MetricReceiver{*metricsAddr, statsd.HandlerFunc(f)}
	go receiver.ListenAndReceive()

	// Start the console(s)
	if *consoleAddr != "" {
		console := statsd.ConsoleServer{*consoleAddr, &aggregator}
		go console.ListenAndServe()
	}
	if *webConsoleAddr != "" {
		console := statsd.WebConsoleServer{*webConsoleAddr, &aggregator}
		go console.ListenAndServe()
	}

	// Listen forever
	select {}
}
