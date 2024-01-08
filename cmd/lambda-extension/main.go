package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"

	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/awslambda/extension"
	"github.com/atlassian/gostatsd/internal/util"
	"github.com/atlassian/gostatsd/pkg/statsd"
	"github.com/atlassian/gostatsd/pkg/transport"
)

var (
	Version   string
	BuildDate string
	GitCommit string
)

const (
	ParamConfigPath = "config-path"

	ParamVerbose = "verbose"

	ParamLambdaFileName = "lambda-entrypoint-name"
)

func init() {
	// Allows setting Version at compile time
	// and perserve the value set
	if Version == "" {
		Version = GetVersion()
	}
}

func GetConfiguration() (*viper.Viper, error) {
	v := viper.New()
	util.InitViper(v, "")

	cmd := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	cmd.Bool(ParamVerbose, false, "Enables debug level logs within the extension")
	cmd.String(ParamConfigPath, "", "Path to a configuration file")
	cmd.String(ParamLambdaFileName, "", "Name of executable that boostraps lambda. Used as Lambda-Extension-Name header")

	gostatsd.AddFlags(cmd)

	cmd.VisitAll(func(f *pflag.Flag) {
		if err := v.BindPFlag(f.Name, f); err != nil {
			panic(err)
		}
	})

	if err := cmd.Parse(os.Args[1:]); err != nil {
		return nil, err
	}

	if confPath := v.GetString(ParamConfigPath); confPath != "" {
		v.SetConfigFile(confPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, err
		}
	}

	return v, nil
}

func CreateServer(v *viper.Viper, logger logrus.FieldLogger) (*statsd.Server, error) {
	// HTTP client pool
	pool := transport.NewTransportPool(logger, v)

	// Percentiles
	pt, err := getPercentiles(v.GetStringSlice(gostatsd.ParamPercentThreshold))
	if err != nil {
		return nil, err
	}

	// Set defaults for expiry from the main expiry setting
	v.SetDefault(gostatsd.ParamExpiryIntervalCounter, v.GetDuration(gostatsd.ParamExpiryInterval))
	v.SetDefault(gostatsd.ParamExpiryIntervalGauge, v.GetDuration(gostatsd.ParamExpiryInterval))
	v.SetDefault(gostatsd.ParamExpiryIntervalSet, v.GetDuration(gostatsd.ParamExpiryInterval))
	v.SetDefault(gostatsd.ParamExpiryIntervalTimer, v.GetDuration(gostatsd.ParamExpiryInterval))

	// Create server
	return &statsd.Server{
		Runnables:             nil,
		Backends:              nil,
		CachedInstances:       nil,
		InternalTags:          v.GetStringSlice(gostatsd.ParamInternalTags),
		InternalNamespace:     v.GetString(gostatsd.ParamInternalNamespace),
		DefaultTags:           v.GetStringSlice(gostatsd.ParamDefaultTags),
		Hostname:              gostatsd.Source(v.GetString(gostatsd.ParamHostname)),
		ExpiryIntervalCounter: v.GetDuration(gostatsd.ParamExpiryIntervalCounter),
		ExpiryIntervalGauge:   v.GetDuration(gostatsd.ParamExpiryIntervalGauge),
		ExpiryIntervalSet:     v.GetDuration(gostatsd.ParamExpiryIntervalSet),
		ExpiryIntervalTimer:   v.GetDuration(gostatsd.ParamExpiryIntervalTimer),
		FlushInterval:         v.GetDuration(gostatsd.ParamFlushInterval),
		FlushOffset:           v.GetDuration(gostatsd.ParamFlushOffset),
		FlushAligned:          v.GetBool(gostatsd.ParamFlushAligned),
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
	}, nil
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

func main() {
	conf, err := GetConfiguration()
	if err != nil {
		panic(err)
	}

	log := logrus.New().WithFields(map[string]interface{}{
		"version":   Version,
		"buildDate": BuildDate,
		"gitCommit": GitCommit,
	})

	log.Logger.SetFormatter(&logrus.JSONFormatter{})

	if conf.GetBool(ParamVerbose) {
		log.Logger.SetLevel(logrus.DebugLevel)
	}

	// TODO: Configure log group

	log.Info("Starting extension runtime")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	server, err := CreateServer(conf, log)
	if err != nil {
		log.WithError(err).Panic("Unable to create gostatsd server")
	}

	manager, err := extension.NewManager(
		extension.WithLogger(log),
		extension.WithLambdaFileName(conf.GetString(ParamLambdaFileName)),
		//extension.WithFlushChannel(server.FlushChannel),
	)
	if err != nil {
		log.WithError(err).Panic("Unable to create extension manager")
	}

	if err = manager.Run(ctx, server); err != nil {
		log.WithError(err).Error("Issue trying to init lambda extension")
	}

	log.Info("Shutting down")
}
