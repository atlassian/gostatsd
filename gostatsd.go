package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/jtblin/gostatsd/statsd"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/pflag"
)

var (
	BuildDate string
	GitCommit string
	Version   string
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	s := statsd.NewStatsdServer()
	s.AddFlags(pflag.CommandLine)
	pflag.Parse()

	if s.Version {
		fmt.Printf("Version: %s - Commit: %s - Date: %s\n", Version, GitCommit, BuildDate)
		os.Exit(0)
	}

	if err := s.Run(); err != nil {
		log.Fatalf("%v\n", err)
	}
}
