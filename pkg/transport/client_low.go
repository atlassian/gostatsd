package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync/atomic"

	"github.com/cenkalti/backoff"
	"github.com/sirupsen/logrus"

	"github.com/atlassian/gostatsd/pkg/util"
)

type Client struct {
	messagesFailMarshal  uint64 // atomic - messages which failed to be marshalled
	messagesFailCompress uint64 // atomic - messages which failed to be compressed
	messagesSent         uint64 // atomic - messages successfully sent
	messagesRetried      uint64 // atomic - retries (first attempt is not a retry, final failure is not a retry)
	messagesDropped      uint64 // atomic - final failure, does not include semaphore timeout
	messagesTimedOut     uint64 // atomic - messages timed out waiting for a semaphore slot

	logger        logrus.FieldLogger
	compress      bool
	customHeaders map[string]string
	debugBody     bool
	requestSem    util.Semaphore
	userAgent     string
	backoff       util.BackoffFactory

	Client *http.Client
}

// PostRaw will start a goroutine (semaphore allowing) which attempts to POST the provided data
// to the provided URL.  It is fire-and-forget by design.
func (hc *Client) PostRaw(ctx context.Context, url, contentType, encoding string, headers map[string]string, body []byte) {
	if !hc.requestSem.Acquire(ctx) {
		atomic.AddUint64(&hc.messagesTimedOut, 1)
		return
	}

	go func() {
		defer hc.requestSem.Release()
		hc.do(ctx, url, contentType, encoding, body, headers)
	}()
}

func (hc *Client) do(ctx context.Context, url, contentType, encoding string, body []byte, headers map[string]string) {
	var err error

	bo := hc.backoff()
	doPost := hc.constructPost(ctx, body, url, contentType, encoding, headers)

	for {
		if err = doPost(); err == nil {
			atomic.AddUint64(&hc.messagesSent, 1)
			return
		}

		next := bo.NextBackOff()
		if next == backoff.Stop {
			break
		}

		hc.logger.WithError(err).Warn("failed to send, retrying")

		if !interruptableSleep(ctx, next) {
			break
		}

		atomic.AddUint64(&hc.messagesRetried, 1)
	}

	atomic.AddUint64(&hc.messagesDropped, 1)
	hc.logger.WithError(err).Info("failed to send, giving up")
}

func (hc *Client) constructPost(ctx context.Context, body []byte, url, contentType, encoding string, headers map[string]string) func() error {
	return func() error {
		req, err := http.NewRequest("POST", url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("unable to create http.Request: %v", err)
		}

		req = req.WithContext(ctx)

		// Base headers
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("Content-Encoding", encoding)
		req.Header.Set("User-Agent", hc.userAgent)

		// Caller headers
		for key, value := range headers {
			req.Header.Set(key, value)
		}

		// Custom headers always win
		for key, value := range hc.customHeaders {
			if value == "" { // Provide a way to delete headers
				req.Header.Del(key)
			} else {
				req.Header.Set(key, value)
			}
		}

		// Perform the request
		resp, err := hc.Client.Do(req)
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
