package statsd

import (
	"fmt"
	"net"
	"strings"
)

// DefaultConsoleAddr is the default address on which a ConsoleServer will listen
const DefaultConsoleAddr = ":8126"

// ConsoleServer is an object that listens for telnet connection on a TCP address Addr
// and provides a console interface to a manage a MetricsAggregator
type ConsoleServer struct {
	Addr       string
	Aggregator *MetricAggregator
}

// ListenAndServe listens on the ConsoleServer's TCP network address and then calls Serve
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
// the MetricAggregator
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
	panic("not reached")
}

// consoleConn represents a single ConsoleServer connection
type consoleConn struct {
	conn   net.Conn
	server *ConsoleServer
}

// serve reads from the consoleConn and responds to incoming requests
func (c *consoleConn) serve() {
	defer c.conn.Close()
	buf := make([]byte, 1024)

	for {
		c.conn.Write([]byte("console> "))
		nbytes, err := c.conn.Read(buf)
		if err != nil {
			// Connection has likely closed
			return
		}

		var command string
		var args []string
		var result string

		if parts := strings.Fields(string(buf[:nbytes])); len(parts) > 0 {
			command = parts[0]
			if len(parts) > 1 {
				args = parts[1:]
			}
		}

		switch command {
		case "help":
			result = "Commands: stats, counters, timers, gauges, delcounters, deltimers, delgauges, quit\n"
		case "stats":
			c.server.Aggregator.Lock()
			result = fmt.Sprintf(
				"Invalid messages received: %d\n"+
					"Last message received: %s\n"+
					"Last flush to Graphite: %s\n"+
					"Last error from Graphite: %s\n",
				c.server.Aggregator.stats.BadLines,
				c.server.Aggregator.stats.LastMessage,
				c.server.Aggregator.stats.GraphiteLastFlush,
				c.server.Aggregator.stats.GraphiteLastError)
			c.server.Aggregator.Unlock()
		case "counters":
			c.server.Aggregator.Lock()
			result = fmt.Sprint(c.server.Aggregator.counters)
			c.server.Aggregator.Unlock()
		case "timers":
			c.server.Aggregator.Lock()
			result = fmt.Sprint(c.server.Aggregator.timers)
			c.server.Aggregator.Unlock()
		case "gauges":
			c.server.Aggregator.Lock()
			result = fmt.Sprint(c.server.Aggregator.gauges)
			c.server.Aggregator.Unlock()
		case "delcounters":
			c.server.Aggregator.Lock()
			for _, k := range args {
				delete(c.server.Aggregator.counters, k)
			}
			c.server.Aggregator.Unlock()
		case "deltimers":
			c.server.Aggregator.Lock()
			for _, k := range args {
				delete(c.server.Aggregator.timers, k)
			}
			c.server.Aggregator.Unlock()
		case "delgauges":
			c.server.Aggregator.Lock()
			for _, k := range args {
				delete(c.server.Aggregator.gauges, k)
			}
			c.server.Aggregator.Unlock()
		case "quit":
			result = "goodbye\n"
		default:
			result = fmt.Sprintf("unknown command: %s\n", command)
		}

		c.conn.Write([]byte(result))
		if result == "goodbye\n" {
			return
		}
	}
}
