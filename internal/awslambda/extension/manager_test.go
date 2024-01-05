package extension

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tilinna/clock"

	"github.com/atlassian/gostatsd/internal/awslambda/extension/api"
	"github.com/atlassian/gostatsd/internal/awslambda/extension/telemetry"
	"github.com/atlassian/gostatsd/pkg/fakesocket"
)

type mocked struct {
	delay time.Duration
	erred error

	start chan<- struct{}
	done  chan<- struct{}
}

func (m *mocked) Run(ctx context.Context) error {
	m.start <- struct{}{}
	if m.delay > 0 {
		<-clock.After(ctx, m.delay)
	}
	m.done <- struct{}{}
	return m.erred
}

func InitHandler(tb testing.TB, statusCode int) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// Enforcing correct http method is used
		require.Equal(tb, http.MethodPost, r.Method)
		// Enforcing Incoming headers as per the spec
		require.Contains(tb, r.Header, api.LambdaExtensionNameHeaderKey)
		// Encorcing that payload is correct
		var payload api.RegisterRequestPayload
		assert.NoError(tb, json.NewDecoder(r.Body).Decode(&payload), "Must send a valid payload request")

		switch statusCode {
		case http.StatusOK:
			rw.Header().Set(api.LambdaExtensionIdentifierHeaderKey, tb.Name())
			assert.NoError(tb, json.NewEncoder(rw).Encode(&api.RegisterResponsePayload{
				FunctionName:    "mock-init-handler",
				FunctionVersion: "1.0.0-mock",
				Handler:         tb.Name(),
			}))
		case http.StatusBadRequest, http.StatusForbidden:
			rw.WriteHeader(statusCode)
			assert.NoError(tb, json.NewEncoder(rw).Encode(&api.ErrorResponse{
				Message: "Error failed successfully",
				Type:    "mock-test.error",
			}))
		default:
			rw.WriteHeader(statusCode)
		}
	})
}

func InitErrorHandler(tb testing.TB, statusCode int) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// Enforcing that the correct method is used
		require.Equal(tb, http.MethodPost, r.Method)
		// Enforcing that the headers exist
		require.Contains(tb, r.Header, api.LambdaExtensionIdentifierHeaderKey)
		require.Contains(tb, r.Header, api.LambdaErrorHeaderKey)

		var payload api.ErrorRequest
		assert.NoError(tb, json.NewDecoder(r.Body).Decode(&payload))

		switch statusCode {
		case http.StatusAccepted:
			rw.WriteHeader(statusCode)
			assert.NoError(tb, json.NewEncoder(rw).Encode(&api.StatusResponse{
				Status: "accepted",
			}))
		case http.StatusBadRequest, http.StatusForbidden:
			rw.WriteHeader(statusCode)
			assert.NoError(tb, json.NewEncoder(rw).Encode(&api.ErrorResponse{
				Message: "Unable to process init errored shutdown",
				Type:    "mock-test.error",
			}))
		default:
			rw.WriteHeader(statusCode)
		}
	})
}

func ExitErrorHandler(tb testing.TB, statusCode int) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// Enforcing that the correct method is used
		require.Equal(tb, http.MethodPost, r.Method)
		// Enforcing that the headers exist
		require.Contains(tb, r.Header, api.LambdaExtensionIdentifierHeaderKey)
		require.Contains(tb, r.Header, api.LambdaErrorHeaderKey)

		var payload api.ErrorRequest
		assert.NoError(tb, json.NewDecoder(r.Body).Decode(&payload))

		switch statusCode {
		case http.StatusAccepted:
			rw.WriteHeader(statusCode)
			assert.NoError(tb, json.NewEncoder(rw).Encode(&api.StatusResponse{
				Status: "accepted",
			}))
		case http.StatusBadRequest, http.StatusForbidden:
			rw.WriteHeader(statusCode)
			assert.NoError(tb, json.NewEncoder(rw).Encode(&api.ErrorResponse{
				Message: "Unable to process exit errored shutdown",
				Type:    "mock-test.error",
			}))
		default:
			rw.WriteHeader(statusCode)
		}
	})
}

func EventNextHandler(tb testing.TB, statusCode, shutdownAfter int) http.Handler {
	count := 0
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// Enforcing the correct method is used
		require.Equal(tb, http.MethodGet, r.Method)
		// Enforcing the header exist
		require.Contains(tb, r.Header, api.LambdaExtensionIdentifierHeaderKey)

		switch statusCode {
		case http.StatusOK:
			payload := &api.EventNextPayload{
				EventType:          api.Invoke,
				RequestID:          fmt.Sprint(count),
				InvokedFunctionARN: "local:dev:gostatsd-extension-manager",
			}
			if count > shutdownAfter {
				payload.EventType = api.Shutdown
				payload.ShutdownReason = "Finished running test cases"
			}
			assert.NoError(tb, json.NewEncoder(rw).Encode(payload))
			rw.Header().Set(api.LambdaExtensionEventIdentifer, fmt.Sprint(count))
		case http.StatusForbidden:
			rw.WriteHeader(statusCode)
			assert.NoError(tb, json.NewEncoder(rw).Encode(&api.ErrorResponse{
				Message: "Failed to process next handler",
				Type:    "mock-test.error",
			}))
		default:
			rw.WriteHeader(statusCode)
		}

		count++
	})
}

func TelemetryHandler(tb testing.TB, statusCode int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(tb, http.MethodPut, r.Method)
		require.Contains(tb, r.Header, api.LambdaExtensionIdentifierHeaderKey)
		require.Equal(tb, tb.Name(), r.Header.Get(api.LambdaExtensionIdentifierHeaderKey))
		w.WriteHeader(statusCode)
	})
}

func availableAddr() string {
	l, _ := net.Listen("tcp", ":0")
	defer l.Close()
	return l.Addr().String()
}

func TestManagerRegister(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Scenario   string
		StatusCode int
		Expect     error
	}{
		{
			Scenario:   "Registered to Lambda",
			StatusCode: http.StatusOK,
			Expect:     nil,
		},
		{
			Scenario:   "Invalid register payload sent",
			StatusCode: http.StatusBadRequest,
			Expect:     ErrFailedRegistration,
		},
		{
			Scenario:   "Forbidden request sent to Lambda",
			StatusCode: http.StatusForbidden,
			Expect:     ErrFailedRegistration,
		},
		{
			Scenario:   "Unknown error within lambda",
			StatusCode: http.StatusInternalServerError,
			Expect:     ErrFailedRegistration,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.Scenario, func(t *testing.T) {
			t.Parallel()
			r := mux.NewRouter()
			r.Handle(api.RegisterEndpoint.String(), InitHandler(t, tc.StatusCode))
			r.PathPrefix("/").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				assert.Fail(t, "Trying to access wrong path", r.RequestURI)
			})

			s := httptest.NewServer(r)
			t.Cleanup(s.Close)

			u, err := url.Parse(s.URL)
			require.NoError(t, err, "Must be able to parse URL from httptest server")

			m := &manager{
				log:    logrus.New().WithField("test-case", t.Name()),
				client: s.Client(),
				domain: u.Hostname() + ":" + u.Port(),
				name:   t.Name(),
			}

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			assert.ErrorIs(t, m.register(ctx), tc.Expect)
		})
	}
}

func TestManagerDo(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Scenario        string
		EventStatus     int
		TelemetryStatus int
		ShutdownAfter   int
		MockDelay       time.Duration
		MockError       error

		ExpectError error
	}{
		{
			Scenario:        "Normal operation",
			EventStatus:     http.StatusOK,
			TelemetryStatus: http.StatusOK,
			ShutdownAfter:   3,
			MockDelay:       time.Second,
			MockError:       nil,
			ExpectError:     nil,
		},
		{
			Scenario:        "Operational failure sending heartbeart",
			EventStatus:     http.StatusInternalServerError,
			TelemetryStatus: http.StatusOK,
			ShutdownAfter:   2,
			MockDelay:       time.Second,
			MockError:       nil,
			ExpectError:     ErrIssueProgress,
		},
		{
			Scenario:        "Server has shutdown early without cause",
			EventStatus:     http.StatusOK,
			TelemetryStatus: http.StatusOK,
			ShutdownAfter:   3,
			MockDelay:       0,
			MockError:       nil,
			ExpectError:     ErrServerEarlyExit,
		},
		{
			Scenario:        "Server configuration was wrong resulting in during init",
			EventStatus:     http.StatusOK,
			TelemetryStatus: http.StatusOK,
			ShutdownAfter:   0,
			MockDelay:       0,
			MockError:       fakesocket.ErrClosedConnection,
			ExpectError:     fakesocket.ErrClosedConnection,
		},
		{
			Scenario:        "Server failed throughout runtime and successfully committed to lambda",
			EventStatus:     http.StatusOK,
			TelemetryStatus: http.StatusOK,
			ShutdownAfter:   0,
			MockDelay:       300 * time.Millisecond,
			MockError:       fakesocket.ErrClosedConnection,
			ExpectError:     fakesocket.ErrClosedConnection,
		},
		{
			Scenario:        "Server failed to subscribe to telemetry",
			EventStatus:     http.StatusOK,
			TelemetryStatus: http.StatusInternalServerError,
			ShutdownAfter:   0,
			MockDelay:       300 * time.Millisecond,
			MockError:       nil,
			ExpectError:     ErrFailedTelemetrySubscription,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.Scenario, func(t *testing.T) {
			t.Parallel()

			r := mux.NewRouter()
			r.Handle(api.RegisterEndpoint.String(), InitHandler(t, http.StatusOK))
			r.Handle(api.InitErrorEndpoint.String(), InitErrorHandler(t, http.StatusAccepted))
			r.Handle(api.ExitErrorEndpoint.String(), ExitErrorHandler(t, http.StatusAccepted))
			r.Handle(telemetry.SubscribeEndpoint.String(), TelemetryHandler(t, tc.TelemetryStatus))
			r.Handle(api.EventEndpoint.String(), EventNextHandler(t, tc.EventStatus, tc.ShutdownAfter))
			r.PathPrefix("/").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				assert.Fail(t, "Trying to access wrong path", r.RequestURI)
			})

			s := httptest.NewServer(r)
			t.Cleanup(s.Close)

			u, err := url.Parse(s.URL)
			require.NoError(t, err, "Must be able to parse URL from httptest server")
			log := logrus.New()

			m := &manager{
				log:             log,
				client:          s.Client(),
				domain:          u.Hostname() + ":" + u.Port(),
				name:            t.Name(),
				registeredID:    t.Name(),
				telemetryServer: telemetry.NewServer(log, func() {}, telemetry.WithCustomAddr(availableAddr())),
			}

			start := make(chan struct{}, 1)
			done := make(chan struct{}, 1)

			server := &mocked{delay: tc.MockDelay, erred: tc.MockError, start: start, done: done}
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				<-start
				close(start)
				for {
					select {
					case <-done:
						close(done)
						cancel()
						return
					}
				}
			}()
			assert.ErrorIs(t, m.Run(ctx, server), tc.ExpectError)
		})
	}
}

func TestManagerTelemetrySubscription(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name          string
		serverStatus  int
		expectedError error
	}{
		{
			name:          "Successful subscription",
			serverStatus:  http.StatusOK,
			expectedError: nil,
		},
		{
			name:          "Bad subscription request",
			serverStatus:  http.StatusBadRequest,
			expectedError: ErrFailedTelemetrySubscription,
		},
		{
			name:          "Container error",
			serverStatus:  http.StatusInternalServerError,
			expectedError: ErrFailedTelemetrySubscription,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := mux.NewRouter()
			r.Handle(telemetry.SubscribeEndpoint.String(), TelemetryHandler(t, tc.serverStatus))
			s := httptest.NewServer(r)
			t.Cleanup(s.Close)

			u, err := url.Parse(s.URL)
			require.NoError(t, err, "Must be able to parse URL from httptest server")
			log := logrus.New()

			m := &manager{
				log:             log,
				client:          s.Client(),
				domain:          u.Hostname() + ":" + u.Port(),
				name:            t.Name(),
				registeredID:    t.Name(),
				telemetryServer: telemetry.NewServer(log, func() {}, telemetry.WithCustomAddr(availableAddr())),
			}

			assert.ErrorIs(t, tc.expectedError, m.subscribeToTelemetry(context.Background()))
		})
	}
}
