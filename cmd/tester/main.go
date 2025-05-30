package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/fakesocket"
	"github.com/atlassian/gostatsd/pkg/statsd"
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
	rand.Seed(time.Now().UnixNano())
	s := newServer()
	s.AddFlags(pflag.CommandLine)
	pflag.Parse()

	if s.Version {
		fmt.Printf("Version: %s - Commit: %s - Date: %s\n", Version, GitCommit, BuildDate)
		os.Exit(0)
	}
	if s.Benchmark != 0 {
		server := statsd.Server{
			DefaultTags:           gostatsd.DefaultTags,
			ExpiryIntervalCounter: gostatsd.DefaultExpiryInterval,
			ExpiryIntervalGauge:   gostatsd.DefaultExpiryInterval,
			ExpiryIntervalSet:     gostatsd.DefaultExpiryInterval,
			ExpiryIntervalTimer:   gostatsd.DefaultExpiryInterval,
			FlushInterval:         gostatsd.DefaultFlushInterval,
			MaxReaders:            gostatsd.DefaultMaxReaders,
			MaxWorkers:            gostatsd.DefaultMaxWorkers,
			MaxQueueSize:          gostatsd.DefaultMaxQueueSize,
			PercentThreshold:      gostatsd.DefaultPercentThreshold,
			ReceiveBatchSize:      gostatsd.DefaultReceiveBatchSize,
			ReceiveBufferSize:     gostatsd.DefaultReceiveBufferSize,
			Viper:                 viper.New(),
		}
		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(s.Benchmark)*time.Second)
		defer cancelFunc()
		if s.CPUProfile {
			f, e := os.Create("profile.pprof")
			if e != nil {
				log.Fatal(e)
			}
			defer f.Close()
			_ = pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}
		err := server.RunWithCustomSocket(ctx, fakesocket.Factory)
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			log.Errorf("statsd run failed: %v", err)
		}
	} else if err := s.Run(); err != nil {
		log.Fatalf("%v\n", err)
	}
}
