package gostatsd

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

// benchmarkMetricMapCopy runs a benchmark copying a map of size b.N of every type.  Due to memory
// constraints, b.N is scaled down by an arbitrary amount, and the copy is ran multiple times.
func benchmarkMetricMapCopy(b *testing.B, iterations, nameLimit int) {
	mm := MetricMap{
		Counters: Counters{},
		Timers:   Timers{},
		Gauges:   Gauges{},
		Sets:     Sets{},
	}
	rnd := rand.New(rand.NewSource(0))
	mapSize := b.N / iterations

	// Create counters
	for i := 0; i < mapSize; i++ {
		name := strconv.Itoa(int(rnd.Int31n(int32(nameLimit))))
		c := strconv.Itoa(len(mm.Counters[name]))
		if mm.Counters[name] == nil {
			mm.Counters[name] = map[string]Counter{}
		}
		mm.Counters[name]["tag.value.s."+c] = Counter{}
	}

	// Create timers
	for i := 0; i < mapSize; i++ {
		name := strconv.Itoa(int(rnd.Int31n(int32(nameLimit))))
		c := strconv.Itoa(len(mm.Timers[name]))
		if mm.Timers[name] == nil {
			mm.Timers[name] = map[string]Timer{}
		}
		mm.Timers[name]["tag.value.s."+c] = Timer{}
	}

	// Create gauges
	for i := 0; i < mapSize; i++ {
		name := strconv.Itoa(int(rnd.Int31n(int32(nameLimit))))
		c := strconv.Itoa(len(mm.Gauges[name]))
		if mm.Gauges[name] == nil {
			mm.Gauges[name] = map[string]Gauge{}
		}
		mm.Gauges[name]["tag.value.s."+c] = Gauge{}
	}

	// Create sets
	for i := 0; i < mapSize; i++ {
		name := strconv.Itoa(int(rnd.Int31n(int32(nameLimit))))
		c := strconv.Itoa(len(mm.Sets[name]))
		if mm.Sets[name] == nil {
			mm.Sets[name] = map[string]Set{}
		}
		mm.Sets[name]["tag.value.s."+c] = Set{}
	}

	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < iterations; i++ {
		_ = mm.Copy()
	}
}

func BenchmarkMetricBatch(b *testing.B) {
	for nameLimit := 1000; nameLimit <= 100000; nameLimit *= 10 {
		//for iterations := 1; iterations <= 10; iterations++ {

		// Experimentation indicates that iterations is not too important.  A value of 1 is more costly and indicative
		// of real world, but Travis may not have memory to run the test.
		iterations := 2
		b.Run(fmt.Sprintf("%d-names-%d-iterations", nameLimit, iterations), func(b *testing.B) {
			benchmarkMetricMapCopy(b, iterations, nameLimit)
		})

		//}
	}
}
