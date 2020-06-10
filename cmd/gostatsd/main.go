package main

import (
	"context"
	_ "expvar"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ash2k/stager"
	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/backends"
	"github.com/atlassian/gostatsd/pkg/cachedinstances"
	"github.com/atlassian/gostatsd/pkg/cloudproviders"
	"github.com/atlassian/gostatsd/pkg/statsd"
	"github.com/atlassian/gostatsd/pkg/transport"
	"github.com/atlassian/gostatsd/pkg/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

const (
	// ParamVerbose enables verbose logging.
	ParamVerbose = "verbose"
	// ParamProfile enables profiler endpoint on the specified address and port.
	ParamProfile = "profile"
	// ParamJSON makes logger log in JSON format.
	ParamJSON = "json"
	// ParamConfigPath provides file with configuration.
	ParamConfigPath = "config-path"
	// ParamVersion makes program output its version.
	ParamVersion = "version"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	v, version, err := setupConfiguration()
	if err != nil {
		if err == pflag.ErrHelp {
			return
		}
		logrus.Fatalf("Error while parsing configuration: %v", err)
	}
	if version {
		fmt.Printf("Version: %s - Commit: %s - Date: %s\n", Version, GitCommit, BuildDate)
		return
	}
	if err := run(v); err != nil {
		logrus.Fatalf("%v", err)
	}
}

func run(v *viper.Viper) error {
	profileAddr := v.GetString(ParamProfile)
	if profileAddr != "" {
		go func() {
			logrus.Errorf("Profiler server failed: %v", http.ListenAndServe(profileAddr, nil))
		}()
	}

	logrus.Info("Starting server")
	s, runnables, err := constructServer(v)
	if err != nil {
		return err
	}

	stgr := stager.New()
	defer stgr.Shutdown()
	for _, runnable := range runnables {
		stgr.NextStage().StartWithContext(runnable)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	cancelOnInterrupt(ctx, cancelFunc)

	if err := s.Run(ctx); err != nil && err != context.Canceled {
		return fmt.Errorf("server error: %v", err)
	}
	return nil
}

func constructServer(v *viper.Viper) (*statsd.Server, []gostatsd.Runnable, error) {
	var runnables []gostatsd.Runnable
	// Logger
	logger := logrus.StandardLogger()

	// HTTP client pool
	pool := transport.NewTransportPool(logger, v)

	// Cached instances
	var cachedInstances gostatsd.CachedInstances
	selfIP := gostatsd.UnknownIP
	cloudProviderName := v.GetString(gostatsd.ParamCloudProvider)
	if cloudProviderName == "" {
		logger.Info("No cloud provider specified")
	} else {
		var err error
		cloudProvider, err := cloudproviders.Get(logger, cloudProviderName, v, Version)
		if err != nil {
			return nil, nil, err
		}
		runnables = gostatsd.MaybeAppendRunnable(runnables, cloudProvider)
		selfIPtmp, err := cloudProvider.SelfIP()
		if err != nil {
			logger.WithError(err).Warn("Failed to get self ip")
		} else {
			selfIP = selfIPtmp
		}

		cachedInstances, err = cachedinstances.NewCachedInstancesFromViper(logger, cloudProvider, v)
		if err != nil {
			return nil, nil, err
		}
		runnables = gostatsd.MaybeAppendRunnable(runnables, cachedInstances)
	}
	// Backends
	backendNames := v.GetStringSlice(gostatsd.ParamBackends)
	backendsList := make([]gostatsd.Backend, 0, len(backendNames))
	for _, backendName := range backendNames {
		backend, errBackend := backends.InitBackend(backendName, v, pool)
		if errBackend != nil {
			return nil, nil, errBackend
		}
		backendsList = append(backendsList, backend)
		runnables = gostatsd.MaybeAppendRunnable(runnables, backend)
	}
	// Percentiles
	pt, err := getPercentiles(v.GetStringSlice(gostatsd.ParamPercentThreshold))
	if err != nil {
		return nil, nil, err
	}

	// Set defaults for expiry from the main expiry setting
	v.SetDefault(gostatsd.ParamExpiryIntervalCounter, v.GetDuration(gostatsd.ParamExpiryInterval))
	v.SetDefault(gostatsd.ParamExpiryIntervalGauge, v.GetDuration(gostatsd.ParamExpiryInterval))
	v.SetDefault(gostatsd.ParamExpiryIntervalSet, v.GetDuration(gostatsd.ParamExpiryInterval))
	v.SetDefault(gostatsd.ParamExpiryIntervalTimer, v.GetDuration(gostatsd.ParamExpiryInterval))

	// Create server
	return &statsd.Server{
		Backends:              backendsList,
		CachedInstances:       cachedInstances,
		InternalTags:          v.GetStringSlice(gostatsd.ParamInternalTags),
		InternalNamespace:     v.GetString(gostatsd.ParamInternalNamespace),
		DefaultTags:           v.GetStringSlice(gostatsd.ParamDefaultTags),
		Hostname:              v.GetString(gostatsd.ParamHostname),
		SelfIP:                selfIP,
		ExpiryIntervalCounter: v.GetDuration(gostatsd.ParamExpiryIntervalCounter),
		ExpiryIntervalGauge:   v.GetDuration(gostatsd.ParamExpiryIntervalGauge),
		ExpiryIntervalSet:     v.GetDuration(gostatsd.ParamExpiryIntervalSet),
		ExpiryIntervalTimer:   v.GetDuration(gostatsd.ParamExpiryIntervalTimer),
		FlushInterval:         v.GetDuration(gostatsd.ParamFlushInterval),
		IgnoreHost:            v.GetBool(gostatsd.ParamIgnoreHost),
		MaxReaders:            v.GetInt(gostatsd.ParamMaxReaders),
		MaxParsers:            v.GetInt(gostatsd.ParamMaxParsers),
		MaxWorkers:            v.GetInt(gostatsd.ParamMaxWorkers),
		MaxQueueSize:          v.GetInt(gostatsd.ParamMaxQueueSize),
		MaxConcurrentEvents:   v.GetInt(gostatsd.ParamMaxConcurrentEvents),
		EstimatedTags:         v.GetInt(gostatsd.ParamEstimatedTags),
		MetricsAddr:           v.GetString(gostatsd.ParamMetricsAddr),
		Namespace:             v.GetString(gostatsd.ParamNamespace),
		StatserType:           v.GetString(gostatsd.ParamStatserType),
		PercentThreshold:      pt,
		HeartbeatEnabled:      v.GetBool(gostatsd.ParamHeartbeatEnabled),
		ReceiveBatchSize:      v.GetInt(gostatsd.ParamReceiveBatchSize),
		ConnPerReader:         v.GetBool(gostatsd.ParamConnPerReader),
		ServerMode:            v.GetString(gostatsd.ParamServerMode),
		LogRawMetric:          v.GetBool(gostatsd.ParamLogRawMetric),
		HeartbeatTags: gostatsd.Tags{
			fmt.Sprintf("version:%s", Version),
			fmt.Sprintf("commit:%s", GitCommit),
		},
		DisabledSubTypes:          gostatsd.DisabledSubMetrics(v),
		BadLineRateLimitPerSecond: rate.Limit(v.GetFloat64(gostatsd.ParamBadLinesPerMinute) / 60.0),
		HistogramLimit:            v.GetUint32(gostatsd.ParamTimerHistogramLimit),
		Viper:                     v,
		TransportPool:             pool,
	}, runnables, nil
}

func getPercentiles(s []string) ([]float64, error) {
	percentThresholds := make([]float64, len(s))
	for i, sPercentThreshold := range s {
		pt, err := strconv.ParseFloat(sPercentThreshold, 64)
		if err != nil {
			return nil, err
		}
		percentThresholds[i] = pt
	}
	return percentThresholds, nil
}

// cancelOnInterrupt calls f when os.Interrupt or SIGTERM is received.
func cancelOnInterrupt(ctx context.Context, f context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-ctx.Done():
		case <-c:
			f()
		}
	}()
}

func setupConfiguration() (*viper.Viper, bool, error) {
	v := viper.New()
	defer setupLogger(v) // Apply logging configuration in case of early exit
	util.InitViper(v, "")

	var version bool

	cmd := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)

	cmd.BoolVar(&version, ParamVersion, false, "Print the version and exit")
	cmd.Bool(ParamVerbose, false, "Verbose")
	cmd.Bool(ParamJSON, false, "Log in JSON format")
	cmd.String(ParamProfile, "", "Enable profiler endpoint on the specified address and port")
	cmd.String(ParamConfigPath, "", "Path to the configuration file")

	gostatsd.AddFlags(cmd)

	cmd.VisitAll(func(flag *pflag.Flag) {
		if err := v.BindPFlag(flag.Name, flag); err != nil {
			panic(err) // Should never happen
		}
	})

	if err := cmd.Parse(os.Args[1:]); err != nil {
		return nil, false, err
	}

	configPath := v.GetString(ParamConfigPath)
	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, false, err
		}
	}

	return v, version, nil
}

func setupLogger(v *viper.Viper) {
	if v.GetBool(ParamVerbose) {
		logrus.SetLevel(logrus.DebugLevel)
	}
	if v.GetBool(ParamJSON) {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	}
}
