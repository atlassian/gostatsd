package telemetry

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewServer(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, NewServer())
}

func TestServerStart(t *testing.T) {
	t.Parallel()

	s := NewServer(WithCustomAddr("127.0.0.1:8083"))
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-time.NewTimer(1 * time.Second).C
		cancel()
	}()
	err := s.Start(ctx)
	assert.NoError(t, err)
}

func TestRuntimeDoneHook(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name       string
		payload    []Event
		hookCalled bool
	}{
		{
			name:       "should invoke hook on runtimeDoneEvent",
			payload:    []Event{{RuntimeDoneMsg}},
			hookCalled: true,
		},
		{
			name:       "should not invoke hook",
			payload:    []Event{{"platform.init"}, {"platform.start"}},
			hookCalled: false,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			callbackInvoked := false
			f := func() { callbackInvoked = true }

			p, err := jsoniter.Marshal(tc.payload)
			require.NoError(t, err)

			s := NewServer(WithRuntimeDoneHook(f), WithCustomAddr("127.0.0.1:8083"))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go s.Start(ctx)

			res, err := http.Post(s.Endpoint(), "application/json", bytes.NewReader(p))
			defer res.Body.Close()

			assert.Equal(t, http.StatusOK, res.StatusCode)
			assert.Equal(t, tc.hookCalled, callbackInvoked)
		})
	}
}

func TestServerReturnsCorrectEndpoint(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name             string
		s                *Server
		expectedEndpoint string
	}{
		{
			name:             "default server uses sandbox endpoint",
			s:                NewServer(),
			expectedEndpoint: "http://sandbox:8083/telemetry",
		},
		{
			name:             "custom server address returns correct endpoint",
			s:                NewServer(WithCustomAddr("127.0.0.1:8081")),
			expectedEndpoint: "http://127.0.0.1:8081/telemetry",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expectedEndpoint, tc.s.Endpoint())
		})
	}
}
