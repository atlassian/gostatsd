package extension

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/ash2k/stager/wait"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"github.com/tilinna/clock"
	"go.uber.org/multierr"

	"github.com/atlassian/gostatsd/internal/awslambda/extension/api"
	"github.com/atlassian/gostatsd/internal/awslambda/extension/telemetry"
)

var (
	ErrTerminated                  = errors.New("extension has shutdown")
	ErrFailedRegistration          = errors.New("extension failed to register with lambda")
	ErrIssueProgress               = errors.New("unable continue execution")
	ErrServerEarlyExit             = errors.New("server has shutdown early without cause")
	ErrFailedTelemetrySubscription = errors.New("extension failed to subscribe to lambda telemetry API")
)

// Manager ensures that the lifetime management of the lambda
type Manager interface {
	Run(ctx context.Context, server Server) error
}

// Server is an interface to the gostatsd.Server type,
// the main purpose here is to allow for mocks to be used throughout testing
// and reduce the amount of set up code to test
type Server interface {
	Run(ctx context.Context) error
}

type OptionFunc func(*manager) error

type manager struct {
	log    logrus.FieldLogger
	client *http.Client
	//flushCh      chan struct{}
	//invokeCh     chan struct{}
	domain       string
	name         string
	registeredID string

	telemetryServer *telemetry.Server
}

var _ Manager = (*manager)(nil)

func WithHTTPClient(c *http.Client) OptionFunc {
	return func(m *manager) error {
		if c == nil {
			return errors.New("invalid http client")
		}
		m.client = c
		return nil
	}
}

func WithLogger(log logrus.FieldLogger) OptionFunc {
	return func(m *manager) error {
		if log == nil {
			return errors.New("invalid logger provided")
		}
		m.log = log
		return nil
	}
}

func WithLambdaFileName(fileName string) OptionFunc {
	return func(m *manager) error {
		if fileName == "" {
			return errors.New("invalid name")
		}
		m.name = fileName
		return nil
	}
}

//func WithFlushChannel(flushCh chan struct{}) OptionFunc {
//	return func(m *manager) error {
//		if flushCh == nil {
//			return errors.New("invalid flush channel")
//		}
//		//m.flushCh = flushCh
//		//m.invokeCh = make(chan struct{}, 2)
//		return nil
//	}
//}

func NewManager(opts ...OptionFunc) (Manager, error) {
	name, err := os.Executable()
	if err != nil {
		return nil, err
	}

	log := logrus.New()
	log.SetOutput(io.Discard)

	m := &manager{
		log:    log,
		client: &http.Client{Timeout: 0},
		domain: os.Getenv(api.EnvLambdaAPIKey),
		name:   name,
	}

	m.telemetryServer = telemetry.NewServer(telemetry.WithRuntimeDoneHook(m.onRuntimeDone))

	for _, opt := range opts {
		if err := opt(m); err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (m *manager) Run(parent context.Context, server Server) error {
	var wg wait.Group
	var chErrs = make(chan error, 3)

	// Init Phase of the lambda
	if err := m.register(parent); err != nil {
		return err
	}

	// Start telemetry server
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	wg.StartWithContext(ctx, func(c context.Context) {
		chErrs <- m.telemetryServer.Start(c)
	})

	if err := m.subscribeToTelemetry(ctx); err != nil {
		m.log.WithError(err).Error("error subscribing to lambda telemetry endpoint")
		return err
	}

	// Start gostatsd server
	wg.StartWithContext(ctx, func(c context.Context) {
		err := server.Run(c)
		// Shutting down due to context is an acceptable
		// reason to shutdown and is not considered an error
		// we write all other errors to the channel
		if !errors.Is(err, ctx.Err()) || err == nil {
			chErrs <- err
		}
	})

	select {
	case err := <-chErrs:
		// In the event that the lambda finished early before
		// it had started accepting events without error,
		// It is still considered an error since
		// it was not correctly shutdown
		if err == nil {
			err = ErrServerEarlyExit
		}
		return multierr.Combine(err, m.initError(parent, err))
	case <-clock.After(ctx, 100*time.Millisecond):
		// In the event that statsd server returns an
		// error during the allowed start up time
		// the we can fail the registration
	}

	wg.StartWithContext(ctx, func(c context.Context) {
		chErrs <- m.heartbeat(c, cancel)
	})

	wg.Wait()
	close(chErrs)
	//if m.invokeCh != nil {
	//	close(m.invokeCh)
	//}

	var err error
	for er := range chErrs {
		err = multierr.Append(err, er)
	}

	if err != nil {
		if errors.Is(err, ErrIssueProgress) {
			return err
		}
		// Since context can be closed here, a seperate context is used
		// to control sending exit data to the lambda
		ctx, cancel := clock.TimeoutContext(context.Background(), time.Second)
		defer cancel()

		m.reportExitError(ctx, err)
		return err
	}

	return nil
}

func (m *manager) heartbeat(ctx context.Context, cancel context.CancelFunc) error {
	defer cancel()
	for ctx.Err() == nil {
		//if m.invokeCh != nil {
		//	<-m.invokeCh
		//	<-time.NewTimer(100 * time.Millisecond).C
		//}

		resp, err := m.nextEvent(ctx)
		if err != nil {
			return err
		}

		if resp.EventType == api.Shutdown {
			m.log.WithField("reason", resp.ShutdownReason).Info("Shutting down extention handler")
			break
		}
		m.log.WithFields(map[string]interface{}{
			"request-id":     resp.RequestID,
			"invocation-arn": resp.InvokedFunctionARN,
			"deadline":       resp.Deadline,
		}).Debug("Progressing further with the invocation")
	}

	return nil
}

func (m *manager) register(ctx context.Context) error {
	var buf bytes.Buffer
	var enc = jsoniter.NewEncoder(&buf)

	err := enc.Encode(&api.RegisterRequestPayload{
		Events: []api.Event{api.Invoke, api.Shutdown},
	})

	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, api.RegisterEndpoint.GetUrl(m.domain), &buf)
	if err != nil {
		return err
	}
	req.Header.Set(api.LambdaExtensionNameHeaderKey, m.name)

	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		// All things are going well so far
	default:
		return multierr.Combine(
			fmt.Errorf("issue trying connect to extension with status code %d", resp.StatusCode),
			ErrFailedRegistration,
		)
	}

	id, exist := resp.Header[api.LambdaExtensionIdentifierHeaderKey]
	if !exist {
		return multierr.Combine(
			errors.New("missing required indentifier header in response"),
			ErrFailedRegistration,
		)
	}
	// Once we have successfully registered to the lambda,
	// the id assigned to the process needs to be preserved and sent
	// with future requests
	m.registeredID = id[0]
	var info api.RegisterResponsePayload
	if err := jsoniter.NewDecoder(resp.Body).Decode(&info); err != nil {
		return err
	}
	// Log the registered payload here as an informative means of
	// debugging connecitivity issues in future.
	m.log.WithFields(map[string]interface{}{
		"function-name":    info.FunctionName,
		"function-version": info.FunctionVersion,
		"function-handler": info.Handler,
	}).Info("Successfully registered with Lambda")

	//if m.invokeCh != nil {
	//	m.invokeCh <- struct{}{}
	//}

	return nil
}

func (m *manager) subscribeToTelemetry(ctx context.Context) error {
	b, err := jsoniter.Marshal(telemetry.SubscriptionRequest{
		SchemaVersion: telemetry.ApiVersion,
		Types:         []string{telemetry.PlatformSubscriptionType},
		Destination:   &telemetry.SubscriptionDestination{URI: m.telemetryServer.Endpoint(), Protocol: "HTTP"},
		Buffering:     &telemetry.SubscriptionBufferingConfig{TimeoutMs: telemetry.MinBufferingTimeoutMs},
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, telemetry.SubscribeEndpoint.GetUrl(m.domain), bytes.NewReader(b))
	req.Header.Set(api.LambdaExtensionIdentifierHeaderKey, m.registeredID)
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		m.log.WithError(err).Errorf("received %d response code from telemetry subscription with body %s", resp.StatusCode, string(body))
		return ErrFailedTelemetrySubscription
	}

	m.log.Info("successfully subscribed to telemetry API")

	return nil
}

func (m *manager) onRuntimeDone() {
	//noop for now
	//m.flushCh <- struct{}{}
	//m.invokeCh <- struct{}{}
}

func (m *manager) nextEvent(ctx context.Context) (*api.EventNextPayload, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, api.EventEndpoint.GetUrl(m.domain), http.NoBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set(api.LambdaExtensionIdentifierHeaderKey, m.registeredID)

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// All things are good in the world
	default:
		return nil, multierr.Combine(
			fmt.Errorf("issue handling request with status code %d", resp.StatusCode),
			ErrIssueProgress,
		)
	}

	var payload api.EventNextPayload
	if err := jsoniter.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	return &payload, nil
}

func (m *manager) initError(ctx context.Context, problem error) error {
	var buf bytes.Buffer
	var enc = jsoniter.NewEncoder(&buf)

	err := enc.Encode(api.ErrorRequest{
		Message:    problem.Error(),
		Type:       "init.failed.additional.invocations",
		StackTrace: strings.Fields(string(debug.Stack())),
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, api.InitErrorEndpoint.GetUrl(m.domain), &buf)
	if err != nil {
		return err
	}

	req.Header.Set(api.LambdaExtensionIdentifierHeaderKey, m.registeredID)
	req.Header.Set(api.LambdaErrorHeaderKey, "extension.failed-additional-init")

	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusAccepted {
		return nil
	}

	return fmt.Errorf("issue with sending init error to lambda, status code: %d", resp.StatusCode)
}

func (m *manager) reportExitError(ctx context.Context, problem error) {
	var buf bytes.Buffer
	var enc = jsoniter.NewEncoder(&buf)

	err := enc.Encode(api.ErrorRequest{
		Message:    problem.Error(),
		Type:       "extension.shutdown.failed",
		StackTrace: strings.Fields(string(debug.Stack())),
	})

	if err != nil {
		m.log.WithError(err).Error("error encoding error request to lambda runtime")
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, api.ExitErrorEndpoint.GetUrl(m.domain), &buf)
	if err != nil {
		m.log.WithError(err).Error("error forming http exit error request to lambda runtime")
		return
	}

	req.Header.Set(api.LambdaExtensionIdentifierHeaderKey, m.registeredID)
	req.Header.Set(api.LambdaErrorHeaderKey, "extension.runtime.fault")

	resp, err := m.client.Do(req)
	if err != nil {
		m.log.WithError(err).Error("error submitting exit error to lambda runtime")
		return
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusAccepted {
		m.log.WithField("status code", resp.StatusCode).Error("issue with sending exit error to lambda")
	}
}
