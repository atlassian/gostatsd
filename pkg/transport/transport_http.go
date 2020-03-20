package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// There are other options on http.Transport, but these are the
// ones which have been configured in gostatsd historically.

const paramHttpDialerKeepAlive = "dialer-keep-alive"
const paramHttpDialerTimeout = "dialer-timeout"
const paramHttpEnableHttp2 = "enable-http2"
const paramHttpIdleConnectionTimeout = "idle-connection-timeout"
const paramHttpMaxIdleConnections = "max-idle-connections"
const paramHttpNetwork = "network"
const paramHttpTLSHandshakeTimeout = "tls-handshake-timeout"
const paramHttpResponseHeaderTimeout = "response-header-timeout"

const defaultHttpDialerKeepAlive = 30 * time.Second
const defaultHttpDialerTimeout = 5 * time.Second
const defaultHttpEnableHttp2 = false
const defaultHttpIdleConnectionTimeout = 1 * time.Minute
const defaultHttpMaxIdleConnections = 50
const defaultHttpNetwork = "tcp"
const defaultHttpTLSHandshakeTimeout = 3 * time.Second
const defaultHttpResponseHeaderTimeout = time.Duration(0)

func (tp *TransportPool) newHttpTransport(name string, v *viper.Viper) (*http.Transport, error) {
	v.SetDefault(paramHttpDialerKeepAlive, defaultHttpDialerKeepAlive)
	v.SetDefault(paramHttpDialerTimeout, defaultHttpDialerTimeout)
	v.SetDefault(paramHttpEnableHttp2, defaultHttpEnableHttp2)
	v.SetDefault(paramHttpIdleConnectionTimeout, defaultHttpIdleConnectionTimeout)
	v.SetDefault(paramHttpMaxIdleConnections, defaultHttpMaxIdleConnections)
	v.SetDefault(paramHttpNetwork, defaultHttpNetwork)
	v.SetDefault(paramHttpTLSHandshakeTimeout, defaultHttpTLSHandshakeTimeout)
	v.SetDefault(paramHttpResponseHeaderTimeout, defaultHttpResponseHeaderTimeout)

	dialerKeepAlive := v.GetDuration(paramHttpDialerKeepAlive)
	dialerTimeout := v.GetDuration(paramHttpDialerTimeout)
	enableHttp2 := v.GetBool(paramHttpEnableHttp2)
	idleConnectionTimeout := v.GetDuration(paramHttpIdleConnectionTimeout)
	maxIdleConnections := v.GetInt(paramHttpMaxIdleConnections)
	network := v.GetString(paramHttpNetwork)
	tlsHandshakeTimeout := v.GetDuration(paramHttpTLSHandshakeTimeout)
	responseHeaderTimeout := v.GetDuration(paramHttpResponseHeaderTimeout)

	if dialerKeepAlive < -1 {
		return nil, errors.New(paramHttpDialerKeepAlive + " must be -1, 0, or positive") // -1 = disabled, 0 = keepalives enabled, not configured, >0 = keepalive interval
	}
	if dialerTimeout < 0 {
		return nil, errors.New(paramHttpDialerTimeout + " must not be negative") // 0 = no timeout, but OS may impose a limit
	}
	if idleConnectionTimeout < 0 {
		return nil, errors.New(paramHttpIdleConnectionTimeout + " must not be negative") // 0 = no timeout
	}
	if maxIdleConnections < 0 {
		return nil, errors.New(paramHttpMaxIdleConnections + " must not be negative") // 0 = no limit
	}
	if tlsHandshakeTimeout < 0 {
		return nil, errors.New(paramHttpTLSHandshakeTimeout + " must not be negative") // 0 = no timeout
	}
	if responseHeaderTimeout < 0 {
		return nil, errors.New(paramHttpResponseHeaderTimeout + " must not be negative") // 0 = no timeout
	}

	dialer := &net.Dialer{
		Timeout:   dialerTimeout,
		KeepAlive: dialerKeepAlive,
	}

	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSHandshakeTimeout: tlsHandshakeTimeout,
		TLSClientConfig: &tls.Config{
			// Can't use SSLv3 because of POODLE and BEAST
			// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
			// Can't use TLSv1.1 because of RC4 cipher usage
			MinVersion: tls.VersionTLS12,
		},
		DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
			// replace the network with our own
			return dialer.DialContext(ctx, network, address)
		},
		MaxIdleConns:          maxIdleConnections,
		IdleConnTimeout:       idleConnectionTimeout,
		ResponseHeaderTimeout: responseHeaderTimeout,
	}

	if !enableHttp2 {
		// A non-nil empty map used in TLSNextProto to disable HTTP/2 support in client.
		// https://golang.org/doc/go1.6#http2
		transport.TLSNextProto = map[string](func(string, *tls.Conn) http.RoundTripper){}
	}

	tp.logger.WithFields(logrus.Fields{
		"name":                         name,
		paramHttpDialerKeepAlive:       dialerKeepAlive,
		paramHttpDialerTimeout:         dialerTimeout,
		paramHttpEnableHttp2:           enableHttp2,
		paramHttpIdleConnectionTimeout: idleConnectionTimeout,
		paramHttpMaxIdleConnections:    maxIdleConnections,
		paramHttpNetwork:               network,
		paramHttpTLSHandshakeTimeout:   tlsHandshakeTimeout,
	}).Info("created transport")

	return transport, nil
}
