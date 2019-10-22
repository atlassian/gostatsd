package transport

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/atlassian/gostatsd/pkg/util"
)

const (
	paramTransportClientTimeout       = "client-timeout"
	paramTransportCompress            = "compress"
	paramTransportCustomHeaders       = "custom-headers"
	paramTransportDebugBody           = "debug-body"
	paramTransportMaxParallelRequests = "max-parallel-requests"
	paramTransportType                = "type"
	paramTransportUserAgent           = "user-agent"

	defaultTransportClientTimeout       = 10 * time.Second
	defaultTransportCompress            = true
	defaultTransportDebugBody           = false
	defaultTransportMaxParallelRequests = 1000
	defaultTransportType                = transportTypeHttp
	defaultTransportUserAgent           = "gostatsd"

	transportTypeHttp = "http"
)

// TransportPool creates http.Clients as required, using the provided viper.Viper for configuration.
type TransportPool struct {
	config *viper.Viper
	logger logrus.FieldLogger

	mu      sync.Mutex
	clients map[string]*Client
}

func NewTransportPool(logger logrus.FieldLogger, config *viper.Viper) *TransportPool {
	config.SetDefault("transport.default", map[string]interface{}{})
	return &TransportPool{
		logger:  logger,
		clients: map[string]*Client{},
		config:  config,
	}
}

func (tp *TransportPool) Get(name string) (*Client, error) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	if hc, ok := tp.clients[name]; ok {
		return hc, nil
	}

	hc, err := tp.newClient(name)
	if err == nil {
		tp.clients[name] = hc
	}
	return hc, err
}

func (tp *TransportPool) newClient(name string) (*Client, error) {
	sub := tp.config.Sub("transport." + name)
	if sub == nil {
		tp.logger.WithField("name", name).Warn("request for non-configured transport, using transport.default")
		sub = tp.config.Sub("transport.default")
	}

	sub.SetDefault(paramTransportClientTimeout, defaultTransportClientTimeout)
	sub.SetDefault(paramTransportCompress, defaultTransportCompress)
	sub.SetDefault(paramTransportCustomHeaders, nil) // No good way to express this as a const that I can find.
	sub.SetDefault(paramTransportDebugBody, defaultTransportDebugBody)
	sub.SetDefault(paramTransportMaxParallelRequests, defaultTransportMaxParallelRequests)
	sub.SetDefault(paramTransportType, defaultTransportType)
	sub.SetDefault(paramTransportUserAgent, defaultTransportUserAgent)

	clientTimeout := sub.GetDuration(paramTransportClientTimeout)
	compress := sub.GetBool(paramTransportCompress)
	customHeaders := sub.GetStringMapString(paramTransportCustomHeaders)
	debugBody := sub.GetBool(paramTransportDebugBody)
	maxParallelRequests := sub.GetInt(paramTransportMaxParallelRequests)
	transportType := sub.GetString(paramTransportType)
	userAgent := sub.GetString(paramTransportUserAgent)

	if clientTimeout < 0 {
		return nil, errors.New(paramTransportClientTimeout + " must not be negative") // 0 = no timeout
	}

	backoff, err := util.GetRetryFromViper(sub)
	if err != nil {
		tp.logger.WithField("name", name).WithError(err).Error("failed to load retry policy")
		return nil, err
	}

	var transport *http.Transport

	switch transportType {
	case transportTypeHttp:
		transport, err = tp.newHttpTransport(name, sub)
	default:
		err = errors.New(paramTransportType + " must be http")
	}
	if err != nil {
		return nil, err
	}

	headers := map[string]string{}
	headerKeys := []string{}
	for key, value := range customHeaders {
		headers[http.CanonicalHeaderKey(key)] = value
		headerKeys = append(headerKeys, key)
	}

	tp.logger.WithFields(logrus.Fields{
		"name":                            name,
		paramTransportClientTimeout:       clientTimeout,
		paramTransportCompress:            compress,
		paramTransportCustomHeaders:       headerKeys, // Only log keys in case there's secrets
		paramTransportDebugBody:           debugBody,
		paramTransportMaxParallelRequests: maxParallelRequests,
		paramTransportType:                transportType,
		paramTransportUserAgent:           userAgent,
	}).Info("created client")

	return &Client{
		Client: &http.Client{
			Transport: transport,
			Timeout:   clientTimeout,
		},
		logger:        tp.logger.WithField("transport", name),
		backoff:       backoff,
		compress:      compress,
		customHeaders: customHeaders,
		debugBody:     debugBody,
		requestSem:    util.NewSemaphore(maxParallelRequests),
		userAgent:     userAgent,
	}, nil
}
