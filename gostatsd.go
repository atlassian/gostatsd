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
	// BuildDate is the date when the binary was built
	BuildDate string
	// GitCommit is the commit hash when the binary was built
	GitCommit string
	// Version is the version of the binary
	Version string
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	s := statsd.NewServer()
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
