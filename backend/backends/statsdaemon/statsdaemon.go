package statsdaemon

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"

	backendTypes "github.com/atlassian/gostatsd/backend/types"
	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
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
func (client *client) SendMetrics(ctx context.Context, metrics *types.MetricMap) error {
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

// SendEvent sends events to the statsd master server.
func (client *client) SendEvent(ctx context.Context, e *types.Event) error {
	conn, err := net.Dial("udp", client.addr)
	if err != nil {
		return fmt.Errorf("error connecting to statsd backend: %s", err)
	}
	defer conn.Close()

	_, err = conn.Write(constructEventMessage(e).Bytes())

	return err
}

func constructEventMessage(e *types.Event) *bytes.Buffer {
	text := strings.Replace(e.Text, "\n", "\\n", -1)

	var buf bytes.Buffer
	buf.WriteString("_e{")
	buf.WriteString(strconv.Itoa(len(e.Title)))
	buf.WriteByte(',')
	buf.WriteString(strconv.Itoa(len(text)))
	buf.WriteString("}:")
	buf.WriteString(e.Title)
	buf.WriteByte('|')
	buf.WriteString(text)

	if e.DateHappened != 0 {
		buf.WriteString("|d:")
		buf.WriteString(strconv.FormatInt(e.DateHappened, 10))
	}
	if e.Hostname != "" {
		buf.WriteString("|h:")
		buf.WriteString(e.Hostname)
	}
	if e.AggregationKey != "" {
		buf.WriteString("|k:")
		buf.WriteString(e.AggregationKey)
	}
	if e.SourceTypeName != "" {
		buf.WriteString("|s:")
		buf.WriteString(e.SourceTypeName)
	}
	if e.Priority != types.PriNormal {
		buf.WriteString("|p:")
		buf.WriteString(e.Priority.String())
	}
	if e.AlertType != types.AlertInfo {
		buf.WriteString("|t:")
		buf.WriteString(e.AlertType.String())
	}
	if len(e.Tags) > 0 {
		buf.WriteString("|#")
		buf.WriteString(e.Tags[0])
		for _, tag := range e.Tags[1:] {
			buf.WriteByte(',')
			buf.WriteString(tag)
		}
	}
	return &buf
}

// SampleConfig returns the sample config for the statsd backend.
func (client *client) SampleConfig() string {
	return sampleConfig
}

// NewClient constructs a GraphiteClient object by connecting to an address.
func NewClient(address string) (backendTypes.Backend, error) {
	log.Infof("Backend statsdaemon address: %s", address)
	return &client{address}, nil
}

// NewClientFromViper constructs a statsd client by connecting to an address.
func NewClientFromViper(v *viper.Viper) (backendTypes.Backend, error) {
	return NewClient(v.GetString("statsdaemon.address"))
}

// BackendName returns the name of the backend.
func (client *client) BackendName() string {
	return BackendName
}
