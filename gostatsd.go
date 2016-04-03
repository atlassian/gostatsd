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
	// BuildDate is the date when the binary was built.
	BuildDate string
	// GitCommit is the commit hash when the binary was built.
	GitCommit string
	// Version is the version of the binary.
	Version string
)

const (
	// ParamVerbose enables verbose logging.
	ParamVerbose = "verbose"
	// ParamCPUProfile enables use of profiler and write results to this file.
	ParamCPUProfile = "cpu-profile"
	// ParamJSON makes logger log in JSON format.
	ParamJSON = "json"
	// ParamConfigPath provides file with configuration.
	ParamConfigPath = "config-path"
	// ParamVersion makes program output its version.
	ParamVersion = "version"
)

// EnvPrefix is the prefix of the inspected environment variables.
const EnvPrefix = "GSD" //Go Stats D

func main() {
	v, version, err := setupConfiguration()
	if err != nil {
		if err == pflag.ErrHelp {
			return
		}
		log.Fatalf("Error while parsing configuration: %v", err)
	}
	if version {
		fmt.Printf("Version: %s - Commit: %s - Date: %s\n", Version, GitCommit, BuildDate)
		return
	}
	var exitErr error
	defer func() {
		if exitErr != nil {
			log.Fatalf("%v", exitErr)
		}
	}()
	CPUProfile := v.GetString(ParamCPUProfile)
	if CPUProfile != "" {
		f, err := os.Create(CPUProfile)
		if err != nil {
			augmentErr(&exitErr, fmt.Errorf("Failed to open profile file: %v", err))
			return
		}
		defer func() {
			if err := f.Close(); err != nil {
				augmentErr(&exitErr, fmt.Errorf("Failed to close profile file: %v", err))
			}
		}()
		err = pprof.StartCPUProfile(f)
		if err != nil {
			augmentErr(&exitErr, fmt.Errorf("Failed to start CPU profiler: %v", err))
			return
		}
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
		augmentErr(&exitErr, fmt.Errorf("Server error: %v", err))
	}
}

// augmentErr updates exitErr to point to newErr if exitErr is nil, otherwise logs newErr.
// The idea is to log all errors and fail the program with the first one.
func augmentErr(exitErr *error, newErr error) {
	if *exitErr == nil {
		*exitErr = newErr
	} else {
		log.Errorf("%v", newErr)
	}
}

func toSlice(s string) []string {
	//TODO Remove workaround when https://github.com/spf13/viper/issues/112 is fixed
	return strings.Split(s, ",")
}

// cancelOnInterrupt calls f when os.Interrupt or SIGTERM is received.
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

func setupConfiguration() (*viper.Viper, bool, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.SetEnvPrefix(EnvPrefix)
	v.SetTypeByDefaultValue(true)
	v.AutomaticEnv()

	var version bool

	cmd := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)

	cmd.BoolVar(&version, ParamVersion, false, "Print the version and exit")
	cmd.Bool(ParamVerbose, false, "Verbose")
	cmd.Bool(ParamJSON, false, "Log in JSON format")
	cmd.String(ParamCPUProfile, "", "Use profiler and write results to this file")
	cmd.String(ParamConfigPath, "", "Path to the configuration file")

	statsd.AddFlags(cmd)

	cmd.VisitAll(func(flag *pflag.Flag) {
		if err := v.BindPFlag(flag.Name, flag); err != nil {
			panic(err) // Should never happen
		}
	})

	setupLogger(v) // setup logger from environment vars and flag defaults

	if err := cmd.Parse(os.Args[1:]); err != nil {
		return nil, false, err
	}

	setupLogger(v) // update logger with config from command line flags

	configPath := v.GetString(ParamConfigPath)
	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, false, err
		}
	}

	setupLogger(v) // finally update logger with vars from config

	return v, version, nil
}

func setupLogger(v *viper.Viper) {
	if v.GetBool(ParamVerbose) {
		log.SetLevel(log.DebugLevel)
	}
	if v.GetBool(ParamJSON) {
		log.SetFormatter(&log.JSONFormatter{})
	}
}
