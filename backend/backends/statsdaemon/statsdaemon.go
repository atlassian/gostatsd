package statsdaemon

import (
	"bytes"
	"fmt"
	"net"

	"github.com/jtblin/gostatsd/backend"
	"github.com/jtblin/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
)

const backendName = "statsdaemon"

func init() {
	backend.RegisterBackend(backendName, func() (backend.MetricSender, error) {
		return NewStatsdClient()
	})
}

const sampleConfig = `
[statsdaemon]
	# statsdaemon host or ip address
	address = "statsdaemon-master:6126"
`

// StatsdClient is an object that is used to send messages to a statsd server's UDP interface
type StatsdClient struct {
	addr string
}

// SendMetrics sends the metrics in a MetricsMap to the statsd master server
func (client *StatsdClient) SendMetrics(metrics types.MetricMap) error {
	if metrics.NumStats == 0 {
		return nil
	}
	buf := new(bytes.Buffer)
	types.EachCounter(metrics.Counters, func(key, tagsKey string, counter types.Counter) {
		fmt.Fprintf(buf, "%s:%d|c|#%s\n", key, counter.Value, tagsKey)
	})
	types.EachTimer(metrics.Timers, func(key, tagsKey string, timer types.Timer) {
		for _, tr := range timer.Values {
			fmt.Fprintf(buf, "%s:%f|ms|#%s\n", key, tr, tagsKey)
		}
	})
	types.EachGauge(metrics.Gauges, func(key, tagsKey string, gauge types.Gauge) {
		fmt.Fprintf(buf, "%s:%f|g|#%s\n", key, gauge.Value, tagsKey)
	})

	types.EachSet(metrics.Sets, func(key, tagsKey string, set types.Set) {
		for _, s := range set.Values {
			fmt.Fprintf(buf, "%s:%s|s|#%s\n", key, s, tagsKey)
		}
	})

	log.Debugf("%s", buf.String())
	// TODO: breakdown if too large?
	conn, err := net.Dial("udp", client.addr)
	if err != nil {
		return fmt.Errorf("error connecting to statsd backend: %s", err)
	}
	defer conn.Close()

	_, err = buf.WriteTo(conn)
	if err != nil {
		return fmt.Errorf("error sending to statsd backend: %s", err)
	}
	return nil
}

// SampleConfig returns the sample config for the statsd backend
func (client *StatsdClient) SampleConfig() string {
	return sampleConfig
}

// NewStatsdClient constructs a GraphiteClient object by connecting to an address
func NewStatsdClient() (backend.MetricSender, error) {
	return &StatsdClient{viper.GetString("statsdaemon.address")}, nil
}

func (client *StatsdClient) Name() string {
	return backendName
}
