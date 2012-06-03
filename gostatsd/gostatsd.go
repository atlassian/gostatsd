package main

import (
	"flag"
	"fmt"
	"github.com/kisielk/gostatsd/statsd"
	"log"
	"os"
)

var (
	metricsAddr string
	consoleAddr string
	graphiteAddr string
)

func init() {
	const (
		defaultMetricsAddr = ":8125"
		defaultConsoleAddr = ":8126"
		defaultGraphiteAddr = ""
	)
	flag.StringVar(&metricsAddr, "l", defaultMetricsAddr, "Address on which to listen for metrics (optional)")
	flag.StringVar(&consoleAddr, "c", defaultConsoleAddr, "Address on which to listen for console sessions (optional)")
	flag.StringVar(&graphiteAddr, "g", defaultGraphiteAddr, "Address of the graphite server (required)")
}

func main() {
	flag.Parse()
	if graphiteAddr == "" {
		fmt.Printf("Error: You must provide the address of the graphite server with the -g flag\n")
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		return
	}
	err := statsd.ListenAndServe(metricsAddr, consoleAddr, graphiteAddr)
	if err != nil {
		log.Fatal(err)
	}
}
