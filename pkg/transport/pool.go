package transport

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const paramTransportClientTimeout = "client-timeout"
const paramTransportType = "type"

const defaultTransportClientTimeout = 10 * time.Second
const transportTypeHttp = "http"
const defaultTransportType = transportTypeHttp

// TransportPool creates http.Clients as required, using the provided viper.Viper for configuration.
type TransportPool struct {
	config *viper.Viper
	logger logrus.FieldLogger

	mu      sync.Mutex
	clients map[string]*Client
}

func NewTransportPool(logger logrus.FieldLogger, config *viper.Viper) *TransportPool {
	config.SetDefault("transport.default", viper.New())
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
	if err != nil {
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
	sub.SetDefault(paramTransportType, defaultTransportType)

	clientTimeout := sub.GetDuration(paramTransportClientTimeout)
	transportType := sub.Get(paramTransportType)

	if clientTimeout < 0 {
		return nil, errors.New("client-timeout must not be negative") // 0 = no timeout
	}

	var transport *http.Transport
	var err error

	switch transportType {
	case transportTypeHttp:
		transport, err = tp.newHttpTransport(name, sub)
	default:
		err = errors.New("type must be http")
	}
	if err != nil {
		return nil, err
	}

	tp.logger.WithFields(logrus.Fields{
		"name":                      name,
		paramTransportType:          transportType,
		paramTransportClientTimeout: clientTimeout,
	}).Info("created client")

	return &Client{
		Client: &http.Client{
			Transport: transport,
			Timeout:   clientTimeout,
		},
	}, nil
}
