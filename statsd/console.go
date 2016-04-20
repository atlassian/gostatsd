package statsd

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/kisielk/cmd"
	"golang.org/x/net/context"
)

// DefaultConsoleAddr is the default address on which a ConsoleServer will listen.
const DefaultConsoleAddr = ":8126"

var errClientQuit = errors.New("client quit")

// ConsoleServer is an object that listens for telnet connection on a TCP address Addr
// and provides a console interface to manage statsd server.
type ConsoleServer struct {
	Addr string
	Receiver
	Dispatcher
	Flusher
}

// ListenAndServe listens on the ConsoleServer's TCP network address and then calls Serve.
func (s *ConsoleServer) ListenAndServe(ctx context.Context) error {
	addr := s.Addr
	if addr == "" {
		addr = DefaultConsoleAddr
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()
	return s.Serve(ctx, l)
}

// Serve accepts incoming connections on the listener and serves them a console interface to
// the Dispatcher and Receiver.
func (s *ConsoleServer) Serve(ctx context.Context, l net.Listener) error {
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		console := consoleConn{c, s}
		go console.serve(ctx)
	}
}

// consoleConn represents a single ConsoleServer connection.
type consoleConn struct {
	conn   net.Conn
	server *ConsoleServer
}

// serve reads from the consoleConn and responds to incoming requests.
func (c *consoleConn) serve(ctx context.Context) {
	defer c.conn.Close()

	commands := map[string]cmd.CmdFn{
		"help": func(args []string) (string, error) {
			return "Commands: stats, counters, timers, gauges, delcounters, deltimers, delgauges, quit\n", nil
		},
		"stats": func(args []string) (string, error) {
			receiverStats := c.server.Receiver.GetStats()
			flusherStats := c.server.Flusher.GetStats()
			return fmt.Sprintf(
				"Invalid messages received: %d\n"+
					"Metrics received: %d\n"+
					"Packets received: %d\n"+
					"Last packet received: %s\n"+
					"Last flush to backends: %s\n"+
					"Last error from backends: %s\n",
				receiverStats.BadLines,
				receiverStats.MetricsReceived,
				receiverStats.PacketsReceived,
				receiverStats.LastPacket,
				flusherStats.LastFlush,
				flusherStats.LastFlushError), nil
		},
		"counters": func(args []string) (string, error) {
			return c.printMetrics(ctx, getCounters)
		},
		"timers": func(args []string) (string, error) {
			return c.printMetrics(ctx, getTimers)
		},
		"gauges": func(args []string) (string, error) {
			return c.printMetrics(ctx, getGauges)
		},
		"sets": func(args []string) (string, error) {
			return c.printMetrics(ctx, getSets)
		},
		"delcounters": func(args []string) (string, error) {
			i := c.delete(ctx, args, getCounters)
			return fmt.Sprintf("deleted %d counters\n", i), nil
		},
		"deltimers": func(args []string) (string, error) {
			i := c.delete(ctx, args, getTimers)
			return fmt.Sprintf("deleted %d timers\n", i), nil
		},
		"delgauges": func(args []string) (string, error) {
			i := c.delete(ctx, args, getGauges)
			return fmt.Sprintf("deleted %d gauges\n", i), nil
		},
		"delsets": func(args []string) (string, error) {
			i := c.delete(ctx, args, getSets)
			return fmt.Sprintf("deleted %d sets\n", i), nil
		},
		"quit": func(args []string) (string, error) {
			return "goodbye\n", errClientQuit
		},
	}

	console := cmd.New(commands, c.conn, c.conn)
	console.Prompt = "console> "
	if err := console.Loop(); err != nil && err != context.Canceled && err != context.DeadlineExceeded && err != errClientQuit {
		log.Infof("Problem with console connection: %v", err)
	}
}

func (c *consoleConn) delete(ctx context.Context, keys []string, f mapperFunc) uint32 {
	var counter uint32
	wg := c.server.Dispatcher.Process(ctx, func(m *types.MetricMap) {
		metrics := f(m)
		var i uint32
		for _, k := range keys {
			metrics.Delete(k)
			i++
		}
		atomic.AddUint32(&counter, i)
	})
	wg.Wait() // Wait for all workers to execute function

	return counter
}

type mapperFunc func(*types.MetricMap) types.AggregatedMetrics

func (c *consoleConn) printMetrics(ctx context.Context, f mapperFunc) (string, error) {
	results := make(chan *bytes.Buffer, 16) // Some space to avoid blocking

	wg := c.server.Dispatcher.Process(ctx, func(m *types.MetricMap) {
		buf := new(bytes.Buffer) // We cannot share a buffer because this function is executed concurrently by workers
		_, _ = fmt.Fprintln(buf, f(m))
		select {
		case <-ctx.Done():
		case results <- buf:
		}
	})
	go func() {
		wg.Wait()      // Wait for all workers to execute function
		close(results) // Close the channel to break for loop
	}()
	buf := new(bytes.Buffer)
	for res := range results {
		buf.Write(res.Bytes())
	}
	return buf.String(), nil
}

func getCounters(m *types.MetricMap) types.AggregatedMetrics {
	return m.Counters
}

func getSets(m *types.MetricMap) types.AggregatedMetrics {
	return m.Sets
}

func getGauges(m *types.MetricMap) types.AggregatedMetrics {
	return m.Gauges
}

func getTimers(m *types.MetricMap) types.AggregatedMetrics {
	return m.Timers
}
