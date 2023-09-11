package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/transport"

	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const BackendName = "redis"

// Configurable constants
const NameSpace = "statsd:history"
const HistoryDepth = int64(9600)
const redisAddress = "127.0.0.1:6379"
const redisPassword = ""

// No configurable constants below this line

// Publish this is the struct for the final data being published to the redis
type Publish struct {
	Counters     map[string]interface{} `json:"counters"`
	CounterRates map[string]interface{} `json:"counter_rates"`
	Timers       map[string]interface{} `json:"timers"`
	Gauges       map[string]interface{} `json:"gauges"`
	Sets         map[string]interface{} `json:"sets"`
	PctThreshold []int                  `json:"pctThreshold"`
	TimeStamp    int64                  `json:"timestamp"`
	DateTime     string                 `json:"datetime"`
}

// Client is an object that is used to send messages to stdout.
type Client struct {
	disabledSubtypes gostatsd.TimerSubtypes
	logger           logrus.FieldLogger
}

type RedisClient = *redis.Client

var redisClient RedisClient = redis.NewClient(&redis.Options{
	Addr:     redisAddress,
	Password: redisPassword,
	DB:       0,
})

// NewClientFromViper constructs a Redis backend.
func NewClientFromViper(v *viper.Viper, logger logrus.FieldLogger, pool *transport.TransportPool) (gostatsd.Backend, error) {
	return NewClient(
		gostatsd.DisabledSubMetrics(v),
		logger,
	)
}

// NewClient constructs a Redis backend.
func NewClient(disabled gostatsd.TimerSubtypes, logger logrus.FieldLogger) (*Client, error) {
	return &Client{
		disabledSubtypes: disabled,
		logger:           logger,
	}, nil
}

// SendMetricsAsync prints the metrics in MetricsMap to the Redis Server as JSON.
func (client Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	buf := preparePayload(metrics, &client.disabledSubtypes, client.logger)
	go func() {
		cb([]error{writePayload(buf, client.logger)})
	}()
}

// writePayload function writes the payload to Redis
func writePayload(buf *bytes.Buffer, logger logrus.FieldLogger) (retErr error) {
	// Check if buf is empty
	if buf.Len() == 0 {
		logger.Error("writePayload Rejected: Empty Buffer")
		return nil
	}

	// Check if buf contains an error message
	if strings.HasPrefix(buf.String(), "ERROR:") {
		logger.Error("writePayload Rejected: " + buf.String())
		return nil
	}

	// Check if Redis is available
	_, err := redisClient.Ping().Result()
	if err != nil {
		logger.WithError(err).Error("Redis Went Away")
		return err
	}

	// Write the content of buf to Redis
	_, err = redisClient.LPush(NameSpace, buf.Bytes()).Result()
	if err != nil {
		logger.WithError(err).Error("Redis LPush Error")
		return err
	}

	// Trim the Redis list to HistoryDepth
	_, err = redisClient.LTrim(NameSpace, 0, HistoryDepth).Result()
	if err != nil {
		logger.WithError(err).Error("Redis LTrim Error")
		return err
	}

	return nil
}

func preparePayload(metrics *gostatsd.MetricMap, disabled *gostatsd.TimerSubtypes, logger logrus.FieldLogger) *bytes.Buffer {
	buf := new(bytes.Buffer)
	now := time.Now().Unix()

	// Counters
	PUB := map[string]map[string]interface{}{}
	CounterOut := map[string]interface{}{}
	CounterRatesOut := map[string]interface{}{}
	for key, value := range metrics.Counters {
		for _, red := range value {
			CounterOut[key] = int(red.Value)
			CounterRatesOut[key] = int(red.PerSecond)
		}
	}
	PUB["counters"] = CounterOut
	PUB["counter_rates"] = CounterRatesOut

	TimersOut := map[string]interface{}{}
	// Timers
	for key, value := range metrics.Timers {
		for _, red := range value {
			TimersKEY := map[string]interface{}{}

			//CounterOut[key] = int(red.Value)
			//CounterRatesOut[key] = int(red.PerSecond)
			if !disabled.Lower {
				TimersKEY["lower"] = red.Min
			}
			if !disabled.Upper {
				TimersKEY["upper"] = red.Max
			}
			if !disabled.Count {
				TimersKEY["count"] = red.Count
			}
			if !disabled.CountPerSecond {
				TimersKEY["count_ps"] = red.PerSecond
			}
			if !disabled.Mean {
				TimersKEY["mean"] = red.Mean
			}
			if !disabled.Median {
				TimersKEY["median"] = red.Median
			}
			if !disabled.StdDev {
				TimersKEY["std"] = red.StdDev
			}
			if !disabled.Sum {
				TimersKEY["sum"] = red.Sum
			}
			if !disabled.SumSquares {
				TimersKEY["sum_squares"] = red.SumSquares
			}
			for _, pct := range red.Percentiles {
				TimersKEY[pct.Str] = pct.Float
			}
			TimersOut[key] = TimersKEY
		}
	}
	PUB["timers"] = TimersOut

	//Gauges
	GaugesOut := map[string]interface{}{}
	for key, value := range metrics.Gauges {
		for _, red := range value {
			GaugesOut[key] = red.Value
		}
	}
	PUB["gauges"] = GaugesOut
	SetsOut := map[string]interface{}{}
	for key, value := range metrics.Sets {
		for _, red := range value {
			SetsOut[key] = len(red.Values)
		}
	}

	// Sets
	PUB["sets"] = SetsOut
	ThesholdOut := map[string]interface{}{}
	ThesholdOut["90"] = true
	PUB["pctThreshold"] = ThesholdOut
	t := time.Now()

	Result := &Publish{
		Counters:     CounterOut,
		CounterRates: CounterRatesOut,
		Timers:       TimersOut,
		Gauges:       GaugesOut,
		Sets:         SetsOut,
		PctThreshold: []int{90},
		TimeStamp:    now,
		DateTime:     string(t.Format("Mon Jan _2 2006 15:04:05 GMT+0000 (UTC)")),
	}

	FinCounters, err := json.Marshal(Result)
	if err != nil {
		logger.WithError(err).Error("preparePayload: JSON Marshal Error")
	} else {
		buf.Write(FinCounters)
	}
	// JSON is now in buf, or an error message is in buf
	return buf
}

// SendEvent is a no-op.
func (client Client) SendEvent(ctx context.Context, e *gostatsd.Event) (retErr error) {
	//client.logger.Info("Redis Backend SendEvent: " + e.Title)
	return nil
}

// Name returns the name of the backend.
func (Client) Name() string {
	return BackendName
}
