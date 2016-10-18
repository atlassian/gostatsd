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

	backendTypes "github.com/atlassian/gostatsd/backend/types"
	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	// BackendName is the name of this backend.
	BackendName        = "statsdaemon"
	maxUDPPacketSize   = 1472
	defaultDialTimeout = 5 * time.Second
)

const sampleConfig = `
[statsdaemon]
	# statsdaemon host or ip address
	address = "statsdaemon-master:6126"
`

// sendChannelSize specifies the size of the buffer of a channel between caller goroutine, producing buffers, and the
// goroutine that writes them to the socket.
const sendChannelSize = 1000

// client is an object that is used to send messages to a statsd server's UDP or TCP interface.
type client struct {
	address      string
	dialTimeout  time.Duration
	disableTags  bool
	tcpTransport bool
}

// overflowHandler is invoked when accumulated packed size has reached it's limit of maxUDPPacketSize.
// This function should return a new buffer to be used for the rest of the work (may be the same buffer
// if contents are processed somehow and are no longer needed).
type overflowHandler func(*bytes.Buffer) (*bytes.Buffer, error)

var bufFree = sync.Pool{
	New: func() interface{} {
		buf := new(bytes.Buffer)
		buf.Grow(maxUDPPacketSize)
		return buf
	},
}

// SendMetricsAsync flushes the metrics to the statsd server, preparing payload synchronously but doing the send asynchronously.
func (client *client) SendMetricsAsync(ctx context.Context, metrics *types.MetricMap, cb backendTypes.SendCallback) {
	if metrics.NumStats == 0 {
		cb(nil)
		return
	}

	network := "udp"
	if client.tcpTransport {
		network = "tcp"
	}

	conn, err := net.DialTimeout(network, client.address, client.dialTimeout)
	if err != nil {
		cb([]error{fmt.Errorf("[%s] error connecting: %v", BackendName, err)})
		return
	}

	datagrams := make(chan *bytes.Buffer, sendChannelSize)
	localCtx, cancelFunc := context.WithCancel(ctx)

	go func() {
		var result error
		defer func() {
			if errClose := conn.Close(); errClose != nil && result == nil {
				result = fmt.Errorf("[%s] error closing: %v", BackendName, errClose)
			}
			cb([]error{result})
		}()
		defer cancelFunc() // Tell the processMetrics function to stop if it is still running
		for {
			select {
			case <-localCtx.Done():
				result = localCtx.Err()
				return
			case buf, ok := <-datagrams:
				if !ok {
					return
				}
				_, errWrite := conn.Write(buf.Bytes())
				buf.Reset() // Reset buffer before returning it to the pool
				bufFree.Put(buf)
				if errWrite != nil {
					result = fmt.Errorf("[%s] error sending: %v", BackendName, errWrite)
					return
				}
			}
		}
	}()
	err = processMetrics(metrics, func(buf *bytes.Buffer) (*bytes.Buffer, error) {
		select {
		case <-localCtx.Done(): // This can happen if 1) parent context is Done or 2) receiver encountered an error
			return nil, localCtx.Err()
		case datagrams <- buf:
			return bufFree.Get().(*bytes.Buffer), nil
		}
	}, client.disableTags, client.tcpTransport)
	if err == nil {
		// All metrics sent to the channel and context wasn't cancelled (yet) - consuming goroutine will exit
		// with success (unless ctx gets a cancel after the close, which is also ok)
		close(datagrams)
	} else if err != context.Canceled && err != context.DeadlineExceeded {
		log.Panicf("Unexpected error: %v", err)
	}
}

func processMetrics(metrics *types.MetricMap, handler overflowHandler, disableTags bool, tcpTransport bool) (retErr error) {
	type failure struct {
		err error
	}
	defer func() {
		if r := recover(); r != nil {
			if f, ok := r.(failure); ok {
				retErr = f.err
			} else {
				panic(r)
			}
		}
	}()
	buf := bufFree.Get().(*bytes.Buffer)
	line := bufFree.Get().(*bytes.Buffer)
	writeLine := func(format, name, tags string, value interface{}) error {
		line.Reset()
		if tags == "" || disableTags {
			format += "\n"
			fmt.Fprintf(line, format, name, value)
		} else {
			format += "|#%s\n"
			fmt.Fprintf(line, format, name, value, tags)
		}
		// Make sure we don't go over max udp datagram size
		if buf.Len()+line.Len() > maxUDPPacketSize && !tcpTransport {
			var err error
			if buf, err = handler(buf); err != nil {
				return err
			}
		}
		fmt.Fprint(buf, line)
		return nil
	}
	metrics.Counters.Each(func(key, tagsKey string, counter types.Counter) {
		// do not send statsd stats as they will be recalculated on the master instead
		if !strings.HasPrefix(key, "statsd.") {
			var err error
			if err = writeLine("%s:%d|c", key, tagsKey, counter.Value); err != nil {
				panic(failure{err})
			}
		}
	})
	metrics.Timers.Each(func(key, tagsKey string, timer types.Timer) {
		for _, tr := range timer.Values {
			var err error
			if err = writeLine("%s:%f|ms", key, tagsKey, tr); err != nil {
				panic(failure{err})
			}
		}
	})
	metrics.Gauges.Each(func(key, tagsKey string, gauge types.Gauge) {
		var err error
		if err = writeLine("%s:%f|g", key, tagsKey, gauge.Value); err != nil {
			panic(failure{err})
		}
	})
	metrics.Sets.Each(func(key, tagsKey string, set types.Set) {
		for k := range set.Values {
			var err error
			if err = writeLine("%s:%s|s", key, tagsKey, k); err != nil {
				panic(failure{err})
			}
		}
	})
	var err error
	if buf.Len() > 0 {
		_, err = handler(buf) // Process what's left in the buffer
	}
	return err
}

// SendEvent sends events to the statsd master server.
func (client *client) SendEvent(ctx context.Context, e *types.Event) error {
	network := "udp"
	if client.tcpTransport {
		network = "tcp"
	}

	conn, err := net.DialTimeout(network, client.address, client.dialTimeout)
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
func NewClient(address string, dialTimeout time.Duration, disableTags bool, tcpTransport bool) (backendTypes.Backend, error) {
	if address == "" {
		return nil, fmt.Errorf("[%s] address is required", BackendName)
	}
	if dialTimeout <= 0 {
		return nil, fmt.Errorf("[%s] dialTimeout should be positive", BackendName)
	}
	log.Infof("[%s] address=%s dialTimeout=%s", BackendName, address, dialTimeout)
	return &client{
		address:      address,
		dialTimeout:  dialTimeout,
		disableTags:  disableTags,
		tcpTransport: tcpTransport,
	}, nil
}

// NewClientFromViper constructs a statsd client by connecting to an address.
func NewClientFromViper(v *viper.Viper) (backendTypes.Backend, error) {
	g := getSubViper(v, "statsdaemon")
	g.SetDefault("dial_timeout", defaultDialTimeout)
	g.SetDefault("disable_tags", false)
	g.SetDefault("tcp_transport", false)
	return NewClient(
		g.GetString("address"),
		g.GetDuration("dial_timeout"),
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
