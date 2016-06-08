package main

import (
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/atlassian/gostatsd/backend"
	backendTypes "github.com/atlassian/gostatsd/backend/types"
	"github.com/atlassian/gostatsd/cloudprovider"
	"github.com/atlassian/gostatsd/statsd"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
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
	// ParamProfile enables profiler endpoint on the specified address and port.
	ParamProfile = "profile"
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
	rand.Seed(time.Now().UnixNano())
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

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	cancelOnInterrupt(ctx, cancelFunc)

	profileAddr := v.GetString(ParamProfile)
	if profileAddr != "" {
		go func() {
			log.Errorf("Profiler server failed: %v", http.ListenAndServe(profileAddr, nil))
		}()
	}

	log.Info("Starting server")
	s, err := constructServer(v)
	if err != nil {
		exitErr = err
		return
	}
	if err := s.Run(ctx); err != nil && err != context.Canceled {
		exitErr = fmt.Errorf("server error: %v", err)
	}
}

func constructServer(v *viper.Viper) (*statsd.Server, error) {
	// Cloud provider
	cloud, err := cloudprovider.InitCloudProvider(v.GetString(statsd.ParamCloudProvider), v)
	if err != nil {
		return nil, err
	}
	// Backends
	backendNames := toSlice(v.GetString(statsd.ParamBackends))
	backends := make([]backendTypes.Backend, len(backendNames))
	for i, backendName := range backendNames {
		backend, errBackend := backend.InitBackend(backendName, v)
		if errBackend != nil {
			return nil, errBackend
		}
		backends[i] = backend
	}
	// Percentiles
	pt, err := getPercentiles(toSlice(v.GetString(statsd.ParamPercentThreshold)))
	if err != nil {
		return nil, err
	}
	// Create server
	return &statsd.Server{
		Backends:         backends,
		ConsoleAddr:      v.GetString(statsd.ParamConsoleAddr),
		CloudProvider:    cloud,
		Limiter:          rate.NewLimiter(rate.Limit(v.GetInt(statsd.ParamMaxCloudRequests)), v.GetInt(statsd.ParamBurstCloudRequests)),
		DefaultTags:      toSlice(v.GetString(statsd.ParamDefaultTags)),
		ExpiryInterval:   v.GetDuration(statsd.ParamExpiryInterval),
		FlushInterval:    v.GetDuration(statsd.ParamFlushInterval),
		MaxReaders:       v.GetInt(statsd.ParamMaxReaders),
		MaxWorkers:       v.GetInt(statsd.ParamMaxWorkers),
		MetricsAddr:      v.GetString(statsd.ParamMetricsAddr),
		Namespace:        v.GetString(statsd.ParamNamespace),
		PercentThreshold: pt,
		WebConsoleAddr:   v.GetString(statsd.ParamWebAddr),
		Viper:            v,
	}, nil
}

func toSlice(s string) []string {
	//TODO Remove workaround when https://github.com/spf13/viper/issues/112 is fixed
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
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
	cmd.String(ParamProfile, "", "Enable profiler endpoint on the specified address and port")
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
