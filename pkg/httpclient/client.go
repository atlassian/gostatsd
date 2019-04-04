package httpclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	defaultClientTimeout         = 10 * time.Second
	defaultCompress              = true
	defaultEnableHttp2           = false
	defaultMaxRequestElapsedTime = 30 * time.Second
	defaultMaxRequests           = 1000
	defaultNetwork               = "tcp"
)

type HttpClient struct {
	messagesFailMarshal  uint64 // atomic - messages which failed to be marshalled
	messagesFailCompress uint64 // atomic - messages which failed to be compressed
	messagesSent         uint64 // atomic - messages successfully sent
	messagesRetried      uint64 // atomic - retries (first send is not a retry, final failure is not a retry)
	messagesDropped      uint64 // atomic - final failure
	messagesTimedOut     uint64 // atomic - messages timed out waiting for a semaphore slot

	logger                logrus.FieldLogger
	client                http.Client
	compress              bool
	maxRequestElapsedTime time.Duration
	requestSem            chan struct{}
}

func NewHttpClientFromViper(logger logrus.FieldLogger, v *viper.Viper) (*HttpClient, error) {
	subViper := getSubViper(v, "http-client")
	subViper.SetDefault("client-timeout", defaultClientTimeout)
	subViper.SetDefault("compress", defaultCompress)
	subViper.SetDefault("enable-http2", defaultEnableHttp2)
	subViper.SetDefault("max-requests", defaultMaxRequests)
	subViper.SetDefault("max-request-elapsed-time", defaultMaxRequestElapsedTime)
	subViper.SetDefault("network", defaultNetwork)

	return NewHttpClient(
		logger,
		subViper.GetString("network"),
		subViper.GetInt("max-requests"),
		subViper.GetBool("compress"),
		subViper.GetBool("enable-http2"),
		subViper.GetDuration("client-timeout"),
		subViper.GetDuration("max-request-elapsed-time"),
	)
}

func NewHttpClient(logger logrus.FieldLogger, network string, maxRequests int, compress, enableHttp2 bool, clientTimeout, maxRequestElapsedTime time.Duration) (*HttpClient, error) {
	if maxRequests <= 0 {
		return nil, fmt.Errorf("max-requests must be positive")
	}
	if clientTimeout <= 0 {
		return nil, fmt.Errorf("client-timeout must be positive")
	}
	if maxRequestElapsedTime <= 0 {
		return nil, fmt.Errorf("max-request-elapsed-time must be positive")
	}

	dialer := &net.Dialer{
		Timeout:   5 * time.Second,  // TODO: Make configurable
		KeepAlive: 30 * time.Second, // TODO: Make configurable
	}

	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSHandshakeTimeout: 3 * time.Second, // TODO: Make configurable
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
		MaxIdleConns:    50,              // TODO: Make configurable
		IdleConnTimeout: 1 * time.Minute, // TODO: Make configurable
	}

	if !enableHttp2 {
		// A non-nil empty map used in TLSNextProto to disable HTTP/2 support in client.
		// https://golang.org/doc/go1.6#http2
		transport.TLSNextProto = map[string](func(string, *tls.Conn) http.RoundTripper){}
	}

	requestSem := make(chan struct{}, maxRequests)
	for i := 0; i < maxRequests; i++ {
		requestSem <- struct{}{}
	}

	return &HttpClient{
		logger: logger,
		client: http.Client{
			Transport: transport,
			Timeout:   clientTimeout,
		},
		compress:              compress,
		maxRequestElapsedTime: maxRequestElapsedTime,
		requestSem:            requestSem,
	}, nil
}

func (hc *HttpClient) PostProtobuf(ctx context.Context, message proto.Message, url string) {
	body, err := proto.Marshal(message)
	if err != nil {
		atomic.AddUint64(&hc.messagesFailMarshal, 1)
		return
	}

	encoding := "identity"
	if hc.compress {
		body, err = compress(body)
		encoding = "deflate"
		if err != nil {
			atomic.AddUint64(&hc.messagesFailCompress, 1)
			return
		}
	}

	hc.Post(ctx, url, "application/x-protobuf", encoding, body)
}

// TODO: This
func (hc *HttpClient) PostJson(ctx context.Context, message interface{}, url string) {
	// hc.Post(ctx, url, "application/json", encoding, body)
}

func (hc *HttpClient) Post(ctx context.Context, url, contentType, encoding string, body []byte) {
	if !hc.acquireSem(ctx) {
		atomic.AddUint64(&hc.messagesTimedOut, 1)
		return
	}

	go func() {
		hc.do(ctx, url, contentType, encoding, body)
		hc.releaseSem()
	}()
}

func (hc *HttpClient) do(ctx context.Context, url, contentType, encoding string, body []byte) {
	var err error

	doPost := hc.constructPost(ctx, body, encoding, url, contentType)

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = hc.maxRequestElapsedTime
	for {
		if err = doPost(); err == nil {
			atomic.AddUint64(&hc.messagesSent, 1)
			return
		}

		next := b.NextBackOff()
		if next == backoff.Stop {
			atomic.AddUint64(&hc.messagesDropped, 1)
			hc.logger.WithError(err).Info("failed to send, giving up")
			return
		}

		atomic.AddUint64(&hc.messagesRetried, 1)

		if interruptableSleep(ctx, next) {
			return
		}
	}
}

func (hc *HttpClient) constructPost(ctx context.Context, body []byte, encoding, url, contentType string) func() error {
	return func() error {
		headers := map[string]string{
			"Content-Type":     contentType,
			"Content-Encoding": encoding,
			"User-Agent":       "gostatsd",
		}
		req, err := http.NewRequest("POST", url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("unable to create http.Request: %v", err)
		}

		req = req.WithContext(ctx)
		for header, v := range headers {
			req.Header.Set(header, v)
		}
		resp, err := hc.client.Do(req)
		if err != nil {
			return fmt.Errorf("error POSTing: %v", err)
		}
		defer consumeAndClose(resp.Body)

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			bodyStart, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
			hc.logger.WithFields(logrus.Fields{
				"status": resp.StatusCode,
				"body":   string(bodyStart),
			}).Info("failed request")
			return fmt.Errorf("received bad status code %d", resp.StatusCode)
		}
		return nil
	}
}

func (hc *HttpClient) acquireSem(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-hc.requestSem:
		return true
	}
}

func (hc *HttpClient) releaseSem() {
	hc.requestSem <- struct{}{} // will never block
}
