package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"syscall"

	"github.com/jtblin/gostatsd/statsd"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

var (
	// BuildDate is the date when the binary was built
	BuildDate string
	// GitCommit is the commit hash when the binary was built
	GitCommit string
	// Version is the version of the binary
	Version string
)

const (
	ParamVerbose    = "verbose"
	ParamCPUProfile = "cpu-profile"
	ParamJson       = "json"
	ParamConfigPath = "config-path"
	ParamVersion    = "version"
)

const EnvPrefix = "GSD" //Go Stats D

func main() {
	err, v, version := setupConfiguration()
	if err != nil {
		if err == pflag.ErrHelp {
			return
		}
		log.Fatalf("Error while parsing configuration: %v\n", err)
	}
	if version {
		fmt.Printf("Version: %s - Commit: %s - Date: %s\n", Version, GitCommit, BuildDate)
		return
	}
	exitCode := 0
	defer func() {
		if exitCode != 0 {
			os.Exit(exitCode)
		}
	}()
	CPUProfile := v.GetString(ParamCPUProfile)
	if CPUProfile != "" {
		f, err := os.Create(CPUProfile)
		if err != nil {
			log.Fatalf("Failed to open profile file: %v", err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				log.Errorf("Failed to close profile file: %v", err)
			}
		}()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	cancelOnInterrupt(ctx, cancelFunc)

	log.Info("Starting server")
	s := statsd.Server{
		Backends:         toSlice(v.GetString(statsd.ParamBackends)),
		ConsoleAddr:      v.GetString(statsd.ParamConsoleAddr),
		CloudProvider:    v.GetString(statsd.ParamCloudProvider),
		DefaultTags:      toSlice(v.GetString(statsd.ParamDefaultTags)),
		ExpiryInterval:   v.GetDuration(statsd.ParamExpiryInterval),
		FlushInterval:    v.GetDuration(statsd.ParamFlushInterval),
		MaxReaders:       v.GetInt(statsd.ParamMaxReaders),
		MaxWorkers:       v.GetInt(statsd.ParamMaxWorkers),
		MaxMessengers:    v.GetInt(statsd.ParamMaxMessengers),
		MetricsAddr:      v.GetString(statsd.ParamMetricsAddr),
		Namespace:        v.GetString(statsd.ParamNamespace),
		PercentThreshold: toSlice(v.GetString(statsd.ParamPercentThreshold)),
		WebConsoleAddr:   v.GetString(statsd.ParamWebAddr),
		Viper:            v,
	}
	if err := s.Run(ctx); err != nil && err != context.Canceled {
		exitCode = 1
		log.Errorf("%v", err)
	}
}

func toSlice(s string) []string {
	//TODO Remove workaround when https://github.com/spf13/viper/issues/112 is fixed
	return strings.Split(s, ",")
}

// cancelOnInterrupt calls f when os.Interrupt or SIGTERM is received
func cancelOnInterrupt(ctx context.Context, f context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-c:
			f()
		case <-ctx.Done():
		}
	}()
}

func setupConfiguration() (error, *viper.Viper, bool) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.SetEnvPrefix(EnvPrefix)
	v.SetTypeByDefaultValue(true)
	v.AutomaticEnv()

	var version bool

	cmd := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)

	cmd.BoolVar(&version, ParamVersion, false, "Print the version and exit")
	cmd.Bool(ParamVerbose, false, "Verbose")
	cmd.Bool(ParamJson, false, "Log in JSON format")
	cmd.String(ParamCPUProfile, "", "Use profiler and write results to this file")
	cmd.String(ParamConfigPath, "", "Path to the configuration file")

	statsd.AddFlags(cmd)

	cmd.VisitAll(func(flag *pflag.Flag) {
		v.BindPFlag(flag.Name, flag)
	})

	setupLogger(v) // setup logger from environment vars and flag defaults

	if err := cmd.Parse(os.Args[1:]); err != nil {
		return err, nil, false
	}

	setupLogger(v) // update logger with config from command line flags

	configPath := v.GetString(ParamConfigPath)
	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return err, nil, false
		}
	}

	setupLogger(v) // finally update logger with vars from config

	return nil, v, version
}

func setupLogger(v *viper.Viper) {
	if v.GetBool(ParamVerbose) {
		log.SetLevel(log.DebugLevel)
	}
	if v.GetBool(ParamJson) {
		log.SetFormatter(&log.JSONFormatter{})
	}
}
