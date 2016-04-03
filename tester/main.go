package main

import (
	"fmt"
	"os"
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/pflag"
)

var (
	// BuildDate is the date when the binary was built.
	BuildDate string
	// GitCommit is the commit hash that built the binary.
	GitCommit string
	// Version is the version.
	Version string
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	s := newServer()
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
