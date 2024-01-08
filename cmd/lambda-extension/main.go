package main

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
	"os"
	"os/signal"

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

	// Create server in forwarder mode
	return &statsd.Server{
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
		ConnPerReader:     v.GetBool(gostatsd.ParamConnPerReader),
		ServerMode:        GostatsdForwarderMode,
		LogRawMetric:      v.GetBool(gostatsd.ParamLogRawMetric),
		HeartbeatTags: gostatsd.Tags{
			fmt.Sprintf("version:%s", Version),
			fmt.Sprintf("commit:%s", GitCommit),
		},
		BadLineRateLimitPerSecond: rate.Limit(v.GetFloat64(gostatsd.ParamBadLinesPerMinute) / 60.0),
		Viper:                     v,
		TransportPool:             pool,
	}, nil
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

	// TODO: Configure log group

	log.Info("Starting extension runtime")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	manager := extension.NewManager(conf.GetString(ParamLambdaFileName), log)

	server, err := CreateServer(conf, log)
	if err != nil {
		log.WithError(err).Panic("Unable to create gostatsd server")
	}

	if err := manager.Run(ctx, server); err != nil {
		log.WithError(err).Error("Failed trying to run lambda extension")
	}

	log.Info("Shutting down")
}
