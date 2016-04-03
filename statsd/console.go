package statsd

import (
	"fmt"
	"net"

	"github.com/jtblin/gostatsd/types"
	"github.com/kisielk/cmd"
)

// DefaultConsoleAddr is the default address on which a ConsoleServer will listen.
const DefaultConsoleAddr = ":8126"

// ConsoleServer is an object that listens for telnet connection on a TCP address Addr
// and provides a console interface to a manage a MetricAggregator.
type ConsoleServer struct {
	Addr       string
	Aggregator *MetricAggregator
}

// ListenAndServe listens on the ConsoleServer's TCP network address and then calls Serve.
func (s *ConsoleServer) ListenAndServe() error {
	addr := s.Addr
	if addr == "" {
		addr = DefaultConsoleAddr
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(l)
}

// Serve accepts incoming connections on the listener and serves them a console interface to
// the MetricAggregator.
func (s *ConsoleServer) Serve(l net.Listener) error {
	defer l.Close()
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		console := consoleConn{c, s}
		go console.serve()
	}
}

// consoleConn represents a single ConsoleServer connection.
type consoleConn struct {
	conn   net.Conn
	server *ConsoleServer
}

// serve reads from the consoleConn and responds to incoming requests.
func (c *consoleConn) serve() {
	defer c.conn.Close()

	commands := map[string]cmd.CmdFn{
		"help": func(args []string) (string, error) {
			return "Commands: stats, counters, timers, gauges, delcounters, deltimers, delgauges, quit\n", nil
		},
		"stats": func(args []string) (string, error) {
			c.server.Aggregator.Lock()
			defer c.server.Aggregator.Unlock()
			return fmt.Sprintf(
				"Invalid messages received: %d\n"+
					"Last message received: %s\n"+
					"Last flush to backends: %s\n"+
					"Last error from backends: %s\n",
				c.server.Aggregator.Stats.BadLines,
				c.server.Aggregator.Stats.LastMessage,
				c.server.Aggregator.Stats.LastFlush,
				c.server.Aggregator.Stats.LastFlushError), nil
		},
		"counters": func(args []string) (string, error) {
			c.server.Aggregator.Lock()
			defer c.server.Aggregator.Unlock()
			return fmt.Sprintln(c.server.Aggregator.Counters), nil
		},
		"timers": func(args []string) (string, error) {
			c.server.Aggregator.Lock()
			defer c.server.Aggregator.Unlock()
			return fmt.Sprintln(c.server.Aggregator.Timers), nil
		},
		"gauges": func(args []string) (string, error) {
			c.server.Aggregator.Lock()
			defer c.server.Aggregator.Unlock()
			return fmt.Sprintln(c.server.Aggregator.Gauges), nil
		},
		"sets": func(args []string) (string, error) {
			c.server.Aggregator.Lock()
			defer c.server.Aggregator.Unlock()
			return fmt.Sprintln(c.server.Aggregator.Sets), nil
		},
		"delcounters": func(args []string) (string, error) {
			i := c.delete(args, c.server.Aggregator.Counters)
			return fmt.Sprintf("deleted %d counters\n", i), nil
		},
		"deltimers": func(args []string) (string, error) {
			i := c.delete(args, c.server.Aggregator.Timers)
			return fmt.Sprintf("deleted %d timers\n", i), nil
		},
		"delgauges": func(args []string) (string, error) {
			i := c.delete(args, c.server.Aggregator.Gauges)
			return fmt.Sprintf("deleted %d gauges\n", i), nil
		},
		"delsets": func(args []string) (string, error) {
			i := c.delete(args, c.server.Aggregator.Sets)
			return fmt.Sprintf("deleted %d sets\n", i), nil
		},
		"quit": func(args []string) (string, error) {
			return "goodbye\n", fmt.Errorf("client quit")
		},
	}

	console := cmd.New(commands, c.conn, c.conn)
	console.Prompt = "console> "
	console.Loop()
}

func (c *consoleConn) delete(keys []string, metrics types.AggregatedMetrics) int {
	c.server.Aggregator.Lock()
	defer c.server.Aggregator.Unlock()
	i := 0
	for _, k := range keys {
		metrics.Delete(k)
		i++
	}
	return i
}
