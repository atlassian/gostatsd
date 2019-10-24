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

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"
	"github.com/atlassian/gostatsd/pkg/util"
)

// metricTracker is used to hold the current and previous values in a
// metric. Because the majority of the time failures will be zero, or
// only have a short-term bursts of activity, we track the last value
// and trigger sending on change. We'll also repeat a few times after
// a change as we probably only try to send some of the values during
// fault conditions.
type metricTracker struct {
	cur     uint64 // atomic
	prev    uint64
	pending uint64 // number of times to re-send
}

type Client struct {
	messagesDropped      metricTracker // atomic - final failure, does not include semaphore timeout
	messagesFailCompress metricTracker // atomic - messages which failed to be compressed
	messagesFailMarshal  metricTracker // atomic - messages which failed to be marshalled
	messagesRetried      metricTracker // atomic - retries (first attempt is not a retry, final failure is not a retry)
	messagesSent         metricTracker // atomic - messages successfully sent
	messagesTimedOut     metricTracker // atomic - messages timed out waiting for a semaphore slot

	logger        logrus.FieldLogger
	compress      bool
	customHeaders map[string]string
	debugBody     bool
	enableMetrics bool
	requestSem    util.Semaphore
	userAgent     string
	backoff       util.BackoffFactory

	Client *http.Client
}

func sendMetricIfChanged(statser stats.Statser, metricName string, tags gostatsd.Tags, m *metricTracker) {
	v := atomic.LoadUint64(&m.cur)
	if v != m.prev {
		m.prev = v
		m.pending = 3 // send this metric for the next 3 intervals
	}
	if m.pending > 0 {
		m.pending--
		statser.Gauge(metricName, float64(v), tags)
	}
}

func (hc *Client) emitMetrics(statser stats.Statser) {
	if !hc.enableMetrics {
		return
	}
	sendMetricIfChanged(statser, "transport.fail", gostatsd.Tags{"failure:send"}, &hc.messagesDropped)
	sendMetricIfChanged(statser, "transport.fail", gostatsd.Tags{"failure:compress"}, &hc.messagesFailCompress)
	sendMetricIfChanged(statser, "transport.fail", gostatsd.Tags{"failure:marshal"}, &hc.messagesFailMarshal)
	sendMetricIfChanged(statser, "transport.retried", nil, &hc.messagesRetried)
	sendMetricIfChanged(statser, "transport.sent", nil, &hc.messagesSent)
	sendMetricIfChanged(statser, "transport.fail", gostatsd.Tags{"failure:mutex"}, &hc.messagesTimedOut)
}

// PostRaw will start a goroutine (semaphore allowing) which attempts to POST the provided data
// to the provided URL.  It is fire-and-forget by design.
func (hc *Client) PostRaw(ctx context.Context, url, contentType, encoding string, headers map[string]string, body []byte) {
	if !hc.requestSem.Acquire(ctx) {
		atomic.AddUint64(&hc.messagesTimedOut.cur, 1)
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
			atomic.AddUint64(&hc.messagesSent.cur, 1)
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

		atomic.AddUint64(&hc.messagesRetried.cur, 1)
	}

	atomic.AddUint64(&hc.messagesDropped.cur, 1)
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
