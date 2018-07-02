package datadogexp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"fmt"
	"strings"

	"github.com/cenkalti/backoff"
	"github.com/sirupsen/logrus"
	"github.com/tilinna/clock"
)

type bodyWriter struct {
	logger logrus.FieldLogger

	userAgent   string
	upstreamURL string
	apiKey      string
	newBackOff  func() backoff.BackOff

	client *http.Client

	// tracer *tracer

	receiveBody <-chan *renderedBody
}

func newBodyWriter(
	logger logrus.FieldLogger,
	userAgent string,
	upstreamURL string,
	apiKey string,
	client *http.Client,
	newBackOff func() backoff.BackOff,
	receiveBody <-chan *renderedBody,
) (*bodyWriter, error) {

	u, err := url.Parse(upstreamURL)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("api_key", apiKey)
	u.RawQuery = q.Encode()
	u.Path = "/api/v1/series"

	// tracer := newTracer(logger)

	return &bodyWriter{
		logger: logger,
		// tracer:      tracer,
		userAgent:   userAgent,
		upstreamURL: u.String(),
		apiKey:      apiKey,
		client:      client,
		receiveBody: receiveBody,
		newBackOff:  newBackOff,
	}, nil
}

func (bw *bodyWriter) Run(ctx context.Context) {
	bw.logger.Info("Starting")
	defer bw.logger.Info("Terminating")

	for {
		select {
		case <-ctx.Done():
			return
		case renderedBody := <-bw.receiveBody:
			/*
				// Simulate a delay, not perfect because it doesn't keep connections open, etc.
				delay := rand.Int31n(300)
				delay = 0
				bw.logger.Infof("Received body, pausing for %d seconds", delay)

				t := clock.NewTimer(ctx, time.Duration(delay)*time.Second)
				select {
				case <-t.C:
				case <-ctx.Done():
					t.Stop()
					return
				}
			*/
			bw.submitWithRetries(ctx, renderedBody)
		}
	}
}

func (bw *bodyWriter) submitWithRetries(ctx context.Context, renderedBody *renderedBody) {
	backOff := bw.newBackOff()
	for {
		err := bw.submitBody(ctx, renderedBody)
		if err == nil {
			return
		}

		err = errors.New(strings.Replace(err.Error(), bw.apiKey, "*****", -1))

		next := backOff.NextBackOff()
		if next == backoff.Stop {
			bw.logger.WithError(err).Error("Failed to send (hard)")
			return
		}
		bw.logger.WithError(err).Warnf("Failed to send (retry in %v)", next.Seconds())

		t := clock.NewTimer(ctx, next)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

func (bw *bodyWriter) submitBody(ctx context.Context, rb *renderedBody) error {
	// Must not log, only returns errors up.  Keep API key sanitization in one place.
	req, err := http.NewRequest("POST", bw.upstreamURL, bytes.NewReader(rb.body))
	if err != nil {
		return err
	}

	req = req.WithContext(ctx)
	// req = bw.tracer.WithTrace(req)

	req.Header.Set("Content-Type", "application/json")
	// req.Header.Set("DD-Dogstatsd-Version", "5.6.3")
	req.Header.Set("User-Agent", bw.userAgent)
	if rb.compressed {
		req.Header.Set("Content-Encoding", "deflate")
	}

	/*
		// For testing only.  This logs secrets.
		r, _ := httputil.DumpRequest(req, true)
		bw.logger.WithField("req", string(r)).Info("body")
	*/

	resp, err := bw.client.Do(req)
	if err != nil {
		return err
	}

	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusNoContent {
		return errors.New(fmt.Sprintf("bad status code: %d", resp.StatusCode))
	}
	return nil

}
