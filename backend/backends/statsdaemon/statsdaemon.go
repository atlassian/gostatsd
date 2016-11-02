package statsdaemon

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/atlassian/gostatsd/backend/backends"
	backendTypes "github.com/atlassian/gostatsd/backend/types"
	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	// BackendName is the name of this backend.
	BackendName      = "statsdaemon"
	maxUDPPacketSize = 1472
	maxTCPPacketSize = 1 * 1024 * 1024
	// DefaultDialTimeout is the default net.Dial timeout.
	DefaultDialTimeout = 5 * time.Second
	// DefaultWriteTimeout is the default socket write timeout.
	DefaultWriteTimeout = 30 * time.Second
	// sendChannelSize specifies the size of the buffer of a channel between caller goroutine, producing buffers, and the
	// goroutine that writes them to the socket.
	sendChannelSize = 1000
	// maxConcurrentSends is the number of max concurrent SendMetricsAsync calls that can actually make progress.
	// More calls will block. The current implementation uses maximum 1 call.
	maxConcurrentSends = 10
)

const sampleConfig = `
[statsdaemon]
	# statsdaemon host or ip address
	address = "statsdaemon-master:6126"
`

// client is an object that is used to send messages to a statsd server's UDP or TCP interface.
type client struct {
	packetSize  int
	disableTags bool
	sender      backends.Sender
}

// overflowHandler is invoked when accumulated packed size has reached it's limit.
// This function should return a new buffer to be used for the rest of the work (may be the same buffer
// if contents are processed somehow and are no longer needed).
type overflowHandler func(*bytes.Buffer) (buf *bytes.Buffer, stop bool)

func (client *client) Run(ctx context.Context) error {
	return client.sender.Run(ctx)
}

// SendMetricsAsync flushes the metrics to the statsd server, preparing payload synchronously but doing the send asynchronously.
func (client *client) SendMetricsAsync(ctx context.Context, metrics *types.MetricMap, cb backendTypes.SendCallback) {
	if metrics.NumStats == 0 {
		cb(nil)
		return
	}

	sink := make(chan *bytes.Buffer, sendChannelSize)
	select {
	case <-ctx.Done():
		cb([]error{ctx.Err()})
		return
	case client.sender.Sink <- backends.Stream{Cb: cb, Buf: sink}:
	}
	defer close(sink)
	client.processMetrics(metrics, func(buf *bytes.Buffer) (*bytes.Buffer, bool) {
		select {
		case <-ctx.Done():
			return nil, true
		case sink <- buf:
			return client.sender.GetBuffer(), false
		}
	})
}

func (client *client) processMetrics(metrics *types.MetricMap, handler overflowHandler) {
	type stopProcessing struct {
	}
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(stopProcessing); !ok {
				panic(r)
			}
		}
	}()
	buf := client.sender.GetBuffer()
	defer func() {
		// Have to use a closure because buf pointer might change its value later
		client.sender.PutBuffer(buf)
	}()
	line := new(bytes.Buffer)
	writeLine := func(format, name, tags string, value interface{}) {
		line.Reset()
		if tags == "" || client.disableTags {
			format += "\n"
			fmt.Fprintf(line, format, name, value)
		} else {
			format += "|#%s\n"
			fmt.Fprintf(line, format, name, value, tags)
		}
		// Make sure we don't go over max udp datagram size
		if buf.Len()+line.Len() > client.packetSize {
			b, stop := handler(buf)
			if stop {
				panic(stopProcessing{})
			}
			buf = b
		}
		fmt.Fprint(buf, line)
	}
	metrics.Counters.Each(func(key, tagsKey string, counter types.Counter) {
		// do not send statsd stats as they will be recalculated on the master instead
		if !strings.HasPrefix(key, "statsd.") {
			writeLine("%s:%d|c", key, tagsKey, counter.Value)
		}
	})
	metrics.Timers.Each(func(key, tagsKey string, timer types.Timer) {
		for _, tr := range timer.Values {
			writeLine("%s:%f|ms", key, tagsKey, tr)
		}
	})
	metrics.Gauges.Each(func(key, tagsKey string, gauge types.Gauge) {
		writeLine("%s:%f|g", key, tagsKey, gauge.Value)
	})
	metrics.Sets.Each(func(key, tagsKey string, set types.Set) {
		for k := range set.Values {
			writeLine("%s:%s|s", key, tagsKey, k)
		}
	})
	if buf.Len() > 0 {
		b, stop := handler(buf) // Process what's left in the buffer
		if !stop {
			buf = b
		}
	}
}

// SendEvent sends events to the statsd master server.
func (client *client) SendEvent(ctx context.Context, e *types.Event) error {
	conn, err := client.sender.ConnFactory()
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

// NewClient constructs a new statsd backend client.
func NewClient(address string, dialTimeout, writeTimeout time.Duration, disableTags, tcpTransport bool) (backendTypes.Backend, error) {
	if address == "" {
		return nil, fmt.Errorf("[%s] address is required", BackendName)
	}
	if dialTimeout <= 0 {
		return nil, fmt.Errorf("[%s] dialTimeout should be positive", BackendName)
	}
	if writeTimeout < 0 {
		return nil, fmt.Errorf("[%s] writeTimeout should be non-negative", BackendName)
	}
	log.Infof("[%s] address=%s dialTimeout=%s writeTimeout=%s", BackendName, address, dialTimeout, writeTimeout)
	var packetSize int
	var transport string
	if tcpTransport {
		packetSize = maxTCPPacketSize
		transport = "tcp"
	} else {
		packetSize = maxUDPPacketSize
		transport = "udp"
	}
	return &client{
		packetSize:  packetSize,
		disableTags: disableTags,
		sender: backends.Sender{
			ConnFactory: func() (net.Conn, error) {
				return net.DialTimeout(transport, address, dialTimeout)
			},
			Sink: make(chan backends.Stream, maxConcurrentSends),
			BufPool: sync.Pool{
				New: func() interface{} {
					buf := new(bytes.Buffer)
					buf.Grow(packetSize)
					return buf
				},
			},
			WriteTimeout: writeTimeout,
		},
	}, nil
}

// NewClientFromViper constructs a statsd client by connecting to an address.
func NewClientFromViper(v *viper.Viper) (backendTypes.Backend, error) {
	g := getSubViper(v, "statsdaemon")
	g.SetDefault("dial_timeout", DefaultDialTimeout)
	g.SetDefault("write_timeout", DefaultWriteTimeout)
	g.SetDefault("disable_tags", false)
	g.SetDefault("tcp_transport", false)
	return NewClient(
		g.GetString("address"),
		g.GetDuration("dial_timeout"),
		g.GetDuration("write_timeout"),
		g.GetBool("disable_tags"),
		g.GetBool("tcp_transport"),
	)
}

// BackendName returns the name of the backend.
func (client *client) BackendName() string {
	return BackendName
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	return n
}
