package statsd

import (
	"fmt"
	"net"
	"strings"
)

const DefaultConsoleAddr = ":8126"

type ConsoleServer struct {
	Addr       string
	Aggregator *MetricAggregator
}

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

type consoleConn struct {
	conn   net.Conn
	server *ConsoleServer
}

func (c *consoleConn) serve() {
	buf := make([]byte, 1024)

	for {
		nbytes, err := c.conn.Read(buf)
		if err != nil {
			// Connection has likely closed
			return
		}

		var result string
		switch parts := strings.Fields(string(buf[:nbytes])); parts[0] {
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
			for _, k := range parts[1:] {
				delete(c.server.Aggregator.counters, k)
			}
			c.server.Aggregator.Unlock()
		case "deltimers":
			c.server.Aggregator.Lock()
			for _, k := range parts[1:] {
				delete(c.server.Aggregator.timers, k)
			}
			c.server.Aggregator.Unlock()
		case "delgauges":
			c.server.Aggregator.Lock()
			for _, k := range parts[1:] {
				delete(c.server.Aggregator.gauges, k)
			}
			c.server.Aggregator.Unlock()
		case "quit":
			result = "quit"
		default:
			result = fmt.Sprintf("unknown command: %s\n", parts[0])
		}

		if result == "quit" {
			return
		} else {
			c.conn.Write([]byte(result))
		}
	}
}
