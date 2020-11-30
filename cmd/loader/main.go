package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	opts := parseArgs(os.Args[1:])

	pendingWorkers := make(chan struct{}, opts.Workers)
	metricGenerators := make([]*metricGenerator, 0, opts.Workers)
	for i := uint(0); i < opts.Workers; i++ {
		generator := &metricGenerator{
			rnd: rand.New(rand.NewSource(rand.Int63())),
			counters: metricData{
				nameFormat:      fmt.Sprintf("%scounter%s", opts.MetricPrefix, opts.MetricSuffix),
				count:           opts.Counts.Counter / uint64(opts.Workers),
				nameCardinality: opts.NameCard.Counter,
				tagCardinality:  opts.TagCard.Counter,
				valueLimit:      opts.ValueRange.Counter,
			},
			gauges: metricData{
				nameFormat:      fmt.Sprintf("%sgauge%s", opts.MetricPrefix, opts.MetricSuffix),
				count:           opts.Counts.Gauge / uint64(opts.Workers),
				nameCardinality: opts.NameCard.Gauge,
				tagCardinality:  opts.TagCard.Gauge,
				valueLimit:      opts.ValueRange.Gauge,
			},
			sets: metricData{
				nameFormat:      fmt.Sprintf("%sset%s", opts.MetricPrefix, opts.MetricSuffix),
				count:           opts.Counts.Set / uint64(opts.Workers),
				nameCardinality: opts.NameCard.Set,
				tagCardinality:  opts.TagCard.Set,
				valueLimit:      opts.ValueRange.Set,
			},
			timers: metricData{
				nameFormat:      fmt.Sprintf("%stimer%s", opts.MetricPrefix, opts.MetricSuffix),
				count:           opts.Counts.Timer / uint64(opts.Workers),
				nameCardinality: opts.NameCard.Timer,
				tagCardinality:  opts.TagCard.Timer,
				valueLimit:      opts.ValueRange.Timer,
			},
		}
		metricGenerators = append(metricGenerators, generator)
		go sendMetricsWorker(
			opts.Target,
			opts.DatagramSize,
			opts.Rate/opts.Workers,
			generator,
			pendingWorkers,
		)
	}

	runningWorkers := opts.Workers
	statusTicker := time.NewTicker(1 * time.Second)
	for runningWorkers > 0 {
		select {
		case <-pendingWorkers:
			runningWorkers--
		case <-statusTicker.C:
			counters := uint64(0)
			gauges := uint64(0)
			sets := uint64(0)
			timers := uint64(0)
			for _, mg := range metricGenerators {
				counters += atomic.LoadUint64(&mg.counters.count)
				gauges += atomic.LoadUint64(&mg.gauges.count)
				sets += atomic.LoadUint64(&mg.sets.count)
				timers += atomic.LoadUint64(&mg.timers.count)
			}
			fmt.Printf("%d counters, %d gauges, %d sets, %d timers\n", counters, gauges, sets, timers)
		}
	}
}

func sendMetricsWorker(
	address string,
	bufSize uint,
	rate uint,
	generator *metricGenerator,
	chDone chan<- struct{},
) {
	s, err := net.DialTimeout("udp", address, 1*time.Second)
	if err != nil {
		panic(err)
	}

	b := &bytes.Buffer{}

	interval := time.Second / time.Duration(rate)

	next := time.Now().Add(interval)

	sb := &strings.Builder{}
	for generator.next(sb) {
		if uint(b.Len()+sb.Len()) > bufSize {
			timeToFlush := time.Until(next)
			if timeToFlush > 0 {
				time.Sleep(timeToFlush)
			}
			_, err := s.Write(b.Bytes())
			if err != nil {
				fmt.Printf("Pausing for 1 second, error sending packet: %v\n", err)
				time.Sleep(1 * time.Second)
			}
			b.Reset()
			next = next.Add(interval)
		}
		b.WriteString(sb.String())
		sb.Reset()
	}

	if b.Len() > 0 {
		_, err := s.Write(b.Bytes())
		if err != nil {
			panic(err)
		}
	}
	chDone <- struct{}{}
}
