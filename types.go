package gostatsd

import (
	"context"
	"time"
)

// Nanotime is the number of nanoseconds elapsed since January 1, 1970 UTC.
// Get the value with time.Now().UnixNano().
type Nanotime int64

func NanoNow() Nanotime {
	return Nanotime(time.Now().UnixNano())
}

// IP is a v4/v6 IP address.
// We do not use net.IP because it will involve conversion to string and back several times.
type IP string

// UnknownIP is an IP of an unknown source.
const UnknownIP IP = ""

type Wait func()

type TimerSubtypes struct {
	Lower          bool
	LowerPct       bool // pct
	Upper          bool
	UpperPct       bool // pct
	Count          bool
	CountPct       bool // pct
	CountPerSecond bool
	Mean           bool
	MeanPct        bool // pct
	Median         bool
	StdDev         bool
	Sum            bool
	SumPct         bool // pct
	SumSquares     bool
	SumSquaresPct  bool // pct
}

// Runnable is a long running function intended to be launched in a goroutine.
type Runnable func(ctx context.Context)

// Runner exposes a Runnable through an interface
type Runner interface {
	Run(ctx context.Context)
}

// RawMetricHandler is an interface that accepts a Metric for processing.  Raw refers to pre-aggregation, not
// pre-consolidation.
type RawMetricHandler interface {
	DispatchMetric(ctx context.Context, m *Metric)
}

// PipelineHandler can be used to handle metrics and events, it provides an estimate of how many tags it may add.
type PipelineHandler interface {
	RawMetricHandler
	// EstimatedTags returns a guess for how many tags to pre-allocate
	EstimatedTags() int
	// DispatchEvent dispatches event to the next step in a pipeline.
	DispatchEvent(ctx context.Context, e *Event)
	// WaitForEvents waits for all event-dispatching goroutines to finish.
	WaitForEvents()
}
