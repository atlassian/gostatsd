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

const DefaultListenerAddr string = "sandbox:8083"

type RuntimeDoneHook func()

type Server struct {
	addr string
	log  logrus.FieldLogger
	f    RuntimeDoneHook
}

type ServerOpt func(s *Server)

func NewServer(opts ...ServerOpt) *Server {
	log := logrus.StandardLogger()
	log.SetOutput(io.Discard)

	s := &Server{
		log:  log,
		f:    func() {},
		addr: DefaultListenerAddr,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func WithRuntimeDoneHook(f RuntimeDoneHook) ServerOpt {
	return func(s *Server) {
		s.f = f
	}
}

func WithCustomAddr(addr string) ServerOpt {
	return func(s *Server) {
		s.addr = addr
	}
}

func (s *Server) Start(ctx context.Context) error {
	mx := mux.NewRouter()
	mx.HandleFunc("/telemetry", s.runtimeDoneHandler).Methods(http.MethodPost)

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

func (s *Server) runtimeDoneHandler(_ http.ResponseWriter, r *http.Request) {
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
		if p.Type == RuntimeDoneMsg {
			s.f()
		}
	}
}

func (s *Server) Endpoint() string {
	return fmt.Sprintf("http://%s/telemetry", s.addr)
}
