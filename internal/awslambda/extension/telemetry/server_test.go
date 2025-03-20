package telemetry

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewServer(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, NewServer("test:80", logrus.New(), NoopHook()))
}

func TestServerStart(t *testing.T) {
	t.Parallel()

	l, _ := net.Listen("tcp", ":0")
	l.Close()

	s := NewServer(l.Addr().String(), logrus.New(), NoopHook())
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-time.After(1 * time.Second)
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
			payload:    []Event{{RuntimeDone}},
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

			l, _ := net.Listen("tcp", ":0")
			l.Close()

			s := NewServer(l.Addr().String(), logrus.New(), f)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go s.Start(ctx)

			if err := waitForListener(l.Addr().String(), 1*time.Second); err != nil {
				panic(err)
			}

			res, err := http.Post(s.Endpoint(), "application/json", bytes.NewReader(p))
			require.NoError(t, err)
			defer res.Body.Close()

			assert.Equal(t, http.StatusOK, res.StatusCode)
			assert.Equal(t, tc.hookCalled, callbackInvoked)
		})
	}
}

func waitForListener(addr string, timeout time.Duration) error {
	stop := time.Now().Add(timeout)
	var err error
	for time.Now().Before(stop) {
		var c net.Conn
		if c, err = net.Dial("tcp", addr); err == nil {
			_ = c.Close()
			return nil
		} else if !strings.Contains(err.Error(), "connection refused") {
			return err
		}
	}
	return fmt.Errorf("timed out waiting for listener: %w", err)
}

func TestServerReturnsCorrectEndpoint(t *testing.T) {
	t.Parallel()
	s := NewServer(LambdaRuntimeAvailableAddr, logrus.New(), NoopHook())
	assert.Equal(t, "http://sandbox:8083/telemetry", s.Endpoint())
}
