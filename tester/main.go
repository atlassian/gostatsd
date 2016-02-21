package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"time"

	"github.com/jtblin/gostatsd/types"
)

var (
	statsdAddr string
)

const (
	defaultStatsdAddr = ":8125"
)

type MetricDef struct {
	Bucket   string
	Type     types.MetricType
	MinVal   float64
	MaxVal   float64
	MinDelay time.Duration
	MaxDelay time.Duration
}

func init() {
	flag.StringVar(&statsdAddr, "s", defaultStatsdAddr, "address of statsd server")
}

func main() {
	flag.Parse()

	c := make(chan string)
	done := make(chan bool)
	go func() {
		conn, err := net.Dial("udp", statsdAddr)
		if err != nil {
			panic(err)
		}
		for l := range c {
			_, err = fmt.Fprintln(conn, l)
			if err != nil {
				fmt.Println("error sending:", err)
				break
			}
		}
		done <- true
	}()

	var err error
	stdin := bufio.NewReaderSize(os.Stdin, 256)
	for {
		var line []byte
		line, _, err = stdin.ReadLine()
		if err != nil {
			break
		}

		var bucket string
		fmt.Sscanf(string(line), "%s", &bucket)
		go func() {
			for {
				time.Sleep(time.Second * time.Duration(rand.Intn(10)))
				c <- fmt.Sprintf("%s:1|c", bucket)
			}
		}()
	}
	if err != io.EOF {
		panic(err)
	}

	<-done
}
