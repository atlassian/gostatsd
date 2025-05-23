package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/awslambda/extension"
	"github.com/atlassian/gostatsd/internal/awslambda/extension/api"
	"github.com/atlassian/gostatsd/internal/awslambda/extension/telemetry"
	"github.com/atlassian/gostatsd/internal/flush"
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
	ParamConfigPath     = "config-path"
	ParamVerbose        = "verbose"
	ParamLambdaFileName = "lambda-entrypoint-name"

	GostatsdForwarderMode = "forwarder"
)

func GetConfiguration() (*viper.Viper, error) {
	v := viper.New()
	util.InitViper(v, "")

	cmd := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	cmd.Bool(ParamVerbose, false, "Enables debug level logs within the extension")
	cmd.String(ParamConfigPath, "", "Path to a configuration file")
	cmd.String(ParamLambdaFileName, "", "Name of executable that bootstraps lambda. Used as Lambda-Extension-Name header")

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

	// enable manual flush mode by default
	v.SetDefault(gostatsd.ParamLambdaExtensionManualFlush, true)
	v.SetDefault(gostatsd.ParamLambdaExtensionTelemetryAddress, telemetry.LambdaRuntimeAvailableAddr)

	return v, nil
}

func NewServer(v *viper.Viper, logger logrus.FieldLogger) *statsd.Server {
	// create server in forwarder mode
	s := &statsd.Server{
		InternalTags:      v.GetStringSlice(gostatsd.ParamInternalTags),
		InternalNamespace: v.GetString(gostatsd.ParamInternalNamespace),
		DefaultTags:       v.GetStringSlice(gostatsd.ParamDefaultTags),
		Hostname:          gostatsd.Source(v.GetString(gostatsd.ParamHostname)),
		FlushInterval:     v.GetDuration(gostatsd.ParamFlushInterval),
		IgnoreHost:        v.GetBool(gostatsd.ParamIgnoreHost),
		MaxReaders:        v.GetInt(gostatsd.ParamMaxReaders),
		MaxParsers:        v.GetInt(gostatsd.ParamMaxParsers),
		EstimatedTags:     v.GetInt(gostatsd.ParamEstimatedTags),
		MetricsAddr:       v.GetString(gostatsd.ParamMetricsAddr),
		Namespace:         v.GetString(gostatsd.ParamNamespace),
		StatserType:       v.GetString(gostatsd.ParamStatserType),
		HeartbeatEnabled:  v.GetBool(gostatsd.ParamHeartbeatEnabled),
		ReceiveBatchSize:  v.GetInt(gostatsd.ParamReceiveBatchSize),
		ReceiveBufferSize: v.GetInt(gostatsd.ParamReceiveBufferSize),
		ConnPerReader:     v.GetBool(gostatsd.ParamConnPerReader),
		ServerMode:        GostatsdForwarderMode,
		LogRawMetric:      v.GetBool(gostatsd.ParamLogRawMetric),
		HeartbeatTags: gostatsd.Tags{
			fmt.Sprintf("version:%s", Version),
			fmt.Sprintf("commit:%s", GitCommit),
		},
		BadLineRateLimitPerSecond: rate.Limit(v.GetFloat64(gostatsd.ParamBadLinesPerMinute) / 60.0),
		Viper:                     v,
		TransportPool:             transport.NewTransportPool(logger, v),
	}

	if v.GetBool(gostatsd.ParamLambdaExtensionManualFlush) {
		s.ForwarderFlushCoordinator = flush.NewFlushCoordinator()
		// Dynamic headers are disable as they can cause multiple flush notifies per flush
		v.Set("dynamic-header", []string{})
	}

	return s
}

func main() {
	conf, err := GetConfiguration()
	if err != nil {
		panic(err)
	}

	if Version == "" {
		Version = GetVersion()
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

	log.Info("Starting extension runtime")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	server := NewServer(conf, log)

	var opts = make([]extension.ManagerOpt, 0)
	telemetryServerAddr := conf.GetString(gostatsd.ParamLambdaExtensionTelemetryAddress)
	if conf.GetBool(gostatsd.ParamLambdaExtensionManualFlush) {
		log.Info("Starting extension with manual flush")
		opts = append(opts, extension.WithManualFlushEnabled(server.ForwarderFlushCoordinator, telemetryServerAddr))
	}

	manager := extension.NewManager(
		os.Getenv(api.EnvLambdaAPIHostname),
		conf.GetString(ParamLambdaFileName),
		log,
		server,
		opts...,
	)

	if err := manager.Run(ctx); err != nil {
		log.WithError(err).Error("Failed trying to run lambda extension")
	}

	log.Info("Shutting down")
}
