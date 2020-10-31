package web

import (
	"fmt"
	"net/http"

	"log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// TODO: now I have setup the Prometheus metric objects, how do I integrate with gostatsd internal metrics measuring
// tools? How do I run this file, and start writing some tests for this. Need to adhere to the project testing styles.
func start() {
   counter := prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "gostatsd",
			Name:      "HTTP requests counter",
			Help:      "An internal counter, reset on flush. It currently measure HTTP metrics such as http.forwarder.invalid, http.incoming etc.",
		})

	// gauges
	gaugeFlush := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "gostatsd",
			Name:      "gauge (flush)",
			Help:      "A value sent as a gauge with the value reset / calculated / sampled every flush interval",
		})

	gaugeTime := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "gostatsd",
			Name:      "gauge (time)",
			Help:      "A single duration measured in milliseconds and sent as a gauge",
		})

	gaugeCumulative := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "gostatsd",
			Name:      "gauge (cumulative)",
			Help:      "An internal counter sent as a gauge with the value never resetting",
		})

	gaugeSparse := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "gostatsd",
			Name:      "gauge (sparse)",
			Help:      "The same as a cumulative gauge, but data is only sent on change",
      })

	http.Handle("/metrics", promhttp.Handler())

	prometheus.MustRegister(counter)

	prometheus.MustRegister(gaugeFlush)
	prometheus.MustRegister(gaugeTime)
	prometheus.MustRegister(gaugeCumulative)
	prometheus.MustRegister(gaugeSparse)

	port := 8080

	log.Printf("Listening on port: %d\n", port)

	log.Fatal(http.ListenAndServe(fmt.Sprintf("%d", port), nil))
}
