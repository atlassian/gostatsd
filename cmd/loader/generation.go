package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
)

type metricData struct {
	count           uint64 // atomic
	nameFormat      string
	nameCardinality uint
	tagCardinality  []uint
	valueLimit      uint
}

type metricGenerator struct {
	rnd *rand.Rand

	counters metricData
	gauges   metricData
	sets     metricData
	timers   metricData
}

func (md *metricData) genName(sb *strings.Builder, r *rand.Rand) {
	sb.WriteString(fmt.Sprintf(md.nameFormat, r.Intn(int(md.nameCardinality))))
	sb.WriteByte(':')
}

func (md *metricData) genTags(sb *strings.Builder, r *rand.Rand) {
	if len(md.tagCardinality) > 0 {
		sb.WriteString("|#")
		for idx, c := range md.tagCardinality {
			if idx > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(fmt.Sprintf("tag%d:%d", idx, r.Intn(int(c))))
		}
	}
	sb.WriteByte('\n')
}

func (mg *metricGenerator) nextCounter(sb *strings.Builder) {
	atomic.AddUint64(&mg.counters.count, ^uint64(0))
	mg.counters.genName(sb, mg.rnd)
	sb.WriteString(strconv.Itoa(1 + mg.rnd.Intn(int(mg.counters.valueLimit+1))))
	sb.WriteString("|c")
	mg.counters.genTags(sb, mg.rnd)
}

func (mg *metricGenerator) nextGauge(sb *strings.Builder) {
	atomic.AddUint64(&mg.gauges.count, ^uint64(0))
	mg.gauges.genName(sb, mg.rnd)
	sb.WriteString(strconv.Itoa(mg.rnd.Intn(int(mg.gauges.valueLimit))))
	sb.WriteString("|g")
	mg.gauges.genTags(sb, mg.rnd)
}

func (mg *metricGenerator) nextSet(sb *strings.Builder) {
	atomic.AddUint64(&mg.sets.count, ^uint64(0))
	mg.sets.genName(sb, mg.rnd)
	sb.WriteString(strconv.Itoa(mg.rnd.Intn(int(mg.sets.valueLimit))))
	sb.WriteString("|s")
	mg.sets.genTags(sb, mg.rnd)
}

func (mg *metricGenerator) nextTimer(sb *strings.Builder) {
	atomic.AddUint64(&mg.timers.count, ^uint64(0))
	mg.timers.genName(sb, mg.rnd)
	sb.WriteString(strconv.FormatFloat(mg.rnd.Float64()*float64(mg.timers.valueLimit), 'g', -1, 64))
	sb.WriteString("|ms")
	mg.timers.genTags(sb, mg.rnd)
}

func (mg *metricGenerator) next(sb *strings.Builder) bool {
	// We can safely read these non-atomically, because this goroutine is the only one that writes to them.
	total := mg.counters.count + mg.gauges.count + mg.sets.count + mg.timers.count
	if total == 0 {
		return false
	}

	n := uint64(mg.rnd.Int63n(int64(total)))
	if n < mg.counters.count {
		mg.nextCounter(sb)
	} else if n < mg.counters.count+mg.gauges.count {
		mg.nextGauge(sb)
	} else if n < mg.counters.count+mg.gauges.count+mg.sets.count {
		mg.nextSet(sb)
	} else {
		mg.nextTimer(sb)
	}
	return true
}
