package telemetry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
)

type RuntimeDoneHook func()

func NoopHook() RuntimeDoneHook {
	return func() {}
}

type Server struct {
	addr string
	log  logrus.FieldLogger
	f    RuntimeDoneHook
}

type ServerOpt func(*Server)

func NewServer(log logrus.FieldLogger, hook RuntimeDoneHook, opts ...ServerOpt) *Server {
	s := &Server{
		log:  log,
		f:    hook,
		addr: LambdaRuntimeAvailableAddr,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func WithCustomAddr(addr string) ServerOpt {
	return func(s *Server) {
		s.addr = addr
	}
}

func (s *Server) Start(ctx context.Context) error {
	mx := mux.NewRouter()
	mx.HandleFunc("/telemetry", s.eventHandler).Methods(http.MethodPost)

	server := &http.Server{Addr: s.addr, Handler: mx}

	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err := server.Shutdown(c)
		if err != nil {
			s.log.WithError(err).Info("did not shutdown gracefully")
		}
	}()

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.log.WithError(err).Error("server error")
		return err
	}

	s.log.Info("Shutdown telemetry server")

	return nil
}

func (s *Server) eventHandler(_ http.ResponseWriter, r *http.Request) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		s.log.WithError(err).Error("error reading body")
	}

	var telePayload []Event
	err = jsoniter.Unmarshal(b, &telePayload)
	if err != nil {
		s.log.WithError(err).Error("error unmarshaling telemetry request")
	}

	for _, p := range telePayload {
		if p.Type == RuntimeDone {
			s.f()
		}
	}
}

func (s *Server) Endpoint() string {
	return fmt.Sprintf("http://%s/telemetry", s.addr)
}
