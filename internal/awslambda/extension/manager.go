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
)

var (
	ErrTerminated         = errors.New("extension has shutdown")
	ErrFailedRegistration = errors.New("extension failed to register with lambda")
	ErrIssueProgress      = errors.New("unable continue execution")
	ErrServerEarlyExit    = errors.New("server has shutdown early without cause")
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

	domain       string
	name         string
	registeredID string
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

func NewManager(opts ...OptionFunc) (Manager, error) {
	name, err := os.Executable()
	if err != nil {
		return nil, err
	}

	log := logrus.New()
	log.SetOutput(io.Discard)

	m := &manager{
		log:    log,
		client: &http.Client{},
		domain: os.Getenv(api.EnvLambdaAPIKey),
		name:   name,
	}

	for _, opt := range opts {
		if err := opt(m); err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (m *manager) Run(parent context.Context, server Server) error {
	// Init Phase of the lambda
	if err := m.register(parent); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(parent)
	// Start gostatsd server
	var (
		wg   wait.Group
		errs = make(chan error, 2)
	)
	defer cancel()

	wg.StartWithContext(ctx, func(c context.Context) {
		err := server.Run(c)
		// Shutting down due to context is an acceptable
		// reason to shutdown and is not considered an error
		if errors.Is(err, ctx.Err()) {
			err = nil
		}
		errs <- err
	})

	select {
	case err := <-errs:
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
		errs <- m.heartbeat(c, cancel)
	})

	wg.Wait()
	close(errs)

	var err error
	for er := range errs {
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
	for ctx.Err() == nil {
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

	cancel()

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
	return nil
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
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

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
