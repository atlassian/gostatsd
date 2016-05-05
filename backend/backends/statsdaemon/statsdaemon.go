package statsdaemon

import (
	"bytes"
	"fmt"
	"net"
	"strings"

	backendTypes "github.com/atlassian/gostatsd/backend/types"
	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	// BackendName is the name of this backend.
	BackendName      = "statsdaemon"
	maxUDPPacketSize = 1472
)

const sampleConfig = `
[statsdaemon]
	# statsdaemon host or ip address
	address = "statsdaemon-master:6126"
`

// client is an object that is used to send messages to a statsd server's UDP interface.
type client struct {
	addr string
}

func (client *client) write(conn *net.Conn, buf *bytes.Buffer) error {
	_, err := buf.WriteTo(*conn)
	if err != nil {
		return fmt.Errorf("error sending to statsd backend: %s", err)
	}
	return nil
}

func (client *client) writeLine(conn *net.Conn, buf *bytes.Buffer, format, name, tags string, value interface{}) error {
	line := new(bytes.Buffer)
	if tags != "" {
		format += "|#%s"
	}
	format += "\n"
	if tags == "" {
		fmt.Fprintf(line, format, name, value)
	} else {
		fmt.Fprintf(line, format, name, value, tags)
	}
	// Make sure we don't go over max udp datagram size
	if buf.Len()+line.Len() > maxUDPPacketSize {
		if err := client.write(conn, buf); err != nil {
			return err
		}
		buf.Reset()
	}
	fmt.Fprint(buf, line)
	line.Reset()
	return nil
}

func logError(err error) error {
	log.Errorf("Error sending to statsd backend: %s", err)
	return err
}

// SendMetrics sends the metrics in a MetricsMap to the statsd master server.
func (client *client) SendMetrics(metrics types.MetricMap) error {
	if metrics.NumStats == 0 {
		return nil
	}

	conn, err := net.Dial("udp", client.addr)
	if err != nil {
		return fmt.Errorf("error connecting to statsd backend: %s", err)
	}
	defer conn.Close()

	var lastError error
	buf := new(bytes.Buffer)
	metrics.Counters.Each(func(key, tagsKey string, counter types.Counter) {
		// do not send statsd stats as they will be recalculated on the master instead
		if !strings.HasPrefix(key, "statsd.") {
			if err = client.writeLine(&conn, buf, "%s:%d|c", key, tagsKey, counter.Value); err != nil {
				lastError = logError(err)
			}
		}
	})
	metrics.Timers.Each(func(key, tagsKey string, timer types.Timer) {
		for _, tr := range timer.Values {
			if err = client.writeLine(&conn, buf, "%s:%f|ms", key, tagsKey, tr); err != nil {
				lastError = logError(err)
			}
		}
	})
	metrics.Gauges.Each(func(key, tagsKey string, gauge types.Gauge) {
		if err = client.writeLine(&conn, buf, "%s:%f|g", key, tagsKey, gauge.Value); err != nil {
			lastError = logError(err)
		}
	})

	metrics.Sets.Each(func(key, tagsKey string, set types.Set) {
		for k := range set.Values {
			if err = client.writeLine(&conn, buf, "%s:%s|s", key, tagsKey, k); err != nil {
				lastError = logError(err)
			}
		}
	})

	if err = client.write(&conn, buf); err != nil {
		return err
	}

	if lastError != nil {
		return lastError
	}
	return nil
}

// SampleConfig returns the sample config for the statsd backend.
func (client *client) SampleConfig() string {
	return sampleConfig
}

// NewClient constructs a GraphiteClient object by connecting to an address.
func NewClient(address string) (backendTypes.MetricSender, error) {
	log.Infof("Backend statsdaemon address: %s", address)
	return &client{address}, nil
}

// NewClientFromViper constructs a statsd client by connecting to an address.
func NewClientFromViper(v *viper.Viper) (backendTypes.MetricSender, error) {
	return NewClient(v.GetString("statsdaemon.address"))
}

// BackendName returns the name of the backend.
func (client *client) BackendName() string {
	return BackendName
}
