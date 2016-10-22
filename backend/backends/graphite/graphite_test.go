package graphite

import (
	"testing"
	"time"

	"github.com/atlassian/gostatsd/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreparePayload(t *testing.T) {
	type testData struct {
		config  *Config
		metrics *types.MetricMap
		result  []byte
	}
	timestamp := types.Nanotime(time.Unix(123456, 0).UnixNano())

	metrics := &types.MetricMap{
		Counters: types.Counters{
			"stat1": map[string]types.Counter{
				"tag1": {PerSecond: 1.1, Value: 5, Timestamp: timestamp},
			},
		},
		Timers: types.Timers{
			"t1": map[string]types.Timer{
				"baz": {
					Values: []float64{10},
					Percentiles: types.Percentiles{
						types.Percentile{Float: 90, Str: "count_90"},
					},
					Timestamp: timestamp,
				},
			},
		},
		Gauges: types.Gauges{
			"g1": map[string]types.Gauge{
				"baz": {Value: 3, Timestamp: timestamp},
			},
		},
		Sets: types.Sets{
			"users": map[string]types.Set{
				"baz": {
					Values: map[string]struct{}{
						"joe":  {},
						"bob":  {},
						"john": {},
					},
					Timestamp: timestamp,
				},
			},
		},
	}
	input := []testData{
		{
			config: &Config{
			// Use defaults
			},
			metrics: metrics,
			result: []byte("stats_counts.stat1 5 1234\n" +
				"stats.stat1 1.100000 1234\n" +
				"stats.timers.t1.lower 0.000000 1234\n" +
				"stats.timers.t1.upper 0.000000 1234\n" +
				"stats.timers.t1.count 0 1234\n" +
				"stats.timers.t1.count_ps 0.000000 1234\n" +
				"stats.timers.t1.mean 0.000000 1234\n" +
				"stats.timers.t1.median 0.000000 1234\n" +
				"stats.timers.t1.std 0.000000 1234\n" +
				"stats.timers.t1.sum 0.000000 1234\n" +
				"stats.timers.t1.sum_squares 0.000000 1234\n" +
				"stats.timers.t1.count_90 90.000000 1234\n" +
				"stats.gauges.g1 3.000000 1234\n" +
				"stats.sets.users 3 1234\n"),
		},
		{
			config: &Config{
				GlobalPrefix:    addr("gp"),
				PrefixCounter:   addr("pc"),
				PrefixTimer:     addr("pt"),
				PrefixGauge:     addr("pg"),
				PrefixSet:       addr("ps"),
				GlobalSuffix:    addr("gs"),
				LegacyNamespace: addrB(true),
			},
			metrics: metrics,
			result: []byte("stats_counts.stat1.gs 5 1234\n" +
				"stats.stat1.gs 1.100000 1234\n" +
				"stats.timers.t1.lower.gs 0.000000 1234\n" +
				"stats.timers.t1.upper.gs 0.000000 1234\n" +
				"stats.timers.t1.count.gs 0 1234\n" +
				"stats.timers.t1.count_ps.gs 0.000000 1234\n" +
				"stats.timers.t1.mean.gs 0.000000 1234\n" +
				"stats.timers.t1.median.gs 0.000000 1234\n" +
				"stats.timers.t1.std.gs 0.000000 1234\n" +
				"stats.timers.t1.sum.gs 0.000000 1234\n" +
				"stats.timers.t1.sum_squares.gs 0.000000 1234\n" +
				"stats.timers.t1.count_90.gs 90.000000 1234\n" +
				"stats.gauges.g1.gs 3.000000 1234\n" +
				"stats.sets.users.gs 3 1234\n"),
		},
		{
			config: &Config{
				GlobalPrefix:    addr("gp"),
				PrefixCounter:   addr("pc"),
				PrefixTimer:     addr("pt"),
				PrefixGauge:     addr("pg"),
				PrefixSet:       addr("ps"),
				GlobalSuffix:    addr("gs"),
				LegacyNamespace: addrB(false),
			},
			metrics: metrics,
			result: []byte("gp.pc.stat1.count.gs 5 1234\n" +
				"gp.pc.stat1.rate.gs 1.100000 1234\n" +
				"gp.pt.t1.lower.gs 0.000000 1234\n" +
				"gp.pt.t1.upper.gs 0.000000 1234\n" +
				"gp.pt.t1.count.gs 0 1234\n" +
				"gp.pt.t1.count_ps.gs 0.000000 1234\n" +
				"gp.pt.t1.mean.gs 0.000000 1234\n" +
				"gp.pt.t1.median.gs 0.000000 1234\n" +
				"gp.pt.t1.std.gs 0.000000 1234\n" +
				"gp.pt.t1.sum.gs 0.000000 1234\n" +
				"gp.pt.t1.sum_squares.gs 0.000000 1234\n" +
				"gp.pt.t1.count_90.gs 90.000000 1234\n" +
				"gp.pg.g1.gs 3.000000 1234\n" +
				"gp.ps.users.gs 3 1234\n"),
		},
	}
	for i, td := range input {
		c, err := NewClient(td.config)
		require.NoError(t, err)
		cl := c.(*client)
		b := cl.preparePayload(td.metrics, time.Unix(1234, 0))
		assert.Equal(t, string(td.result), b.String(), "test %d", i)
	}
}
