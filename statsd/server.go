package statsd

import (
	"time"
)

func ListenAndServe(metricAddr string, consoleAddr string, graphiteAddr string, flushInterval time.Duration) error {
	graphite, err := NewGraphiteClient(graphiteAddr)
	if err != nil {
		return err
	}

	aggregator := NewMetricAggregator(&graphite, flushInterval)

	f := func(metric Metric) {
		aggregator.MetricChan <- metric
	}
	receiver := MetricReceiver{metricAddr, HandlerFunc(f)}

	console := ConsoleServer{consoleAddr, &aggregator}

	go aggregator.Aggregate()
	go receiver.ListenAndReceive()
	go console.ListenAndServe()
	select {}
	return nil
}
