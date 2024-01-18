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
	log        logrus.FieldLogger
	f          RuntimeDoneHook
	httpServer *http.Server
}

func NewServer(addr string, log logrus.FieldLogger, hook RuntimeDoneHook) *Server {
	ts := &Server{
		log: log,
		f:   hook,
	}

	mx := mux.NewRouter()
	mx.HandleFunc("/telemetry", ts.eventHandler).Methods(http.MethodPost)

	ts.httpServer = &http.Server{Addr: addr, Handler: mx}

	return ts
}

func (s *Server) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err := s.httpServer.Shutdown(c)
		if err != nil {
			s.log.WithError(err).Info("Did not shutdown gracefully")
		}
	}()

	s.log.WithFields(map[string]interface{}{
		"serverAddress": s.httpServer.Addr,
	}).Info("starting server")

	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.log.WithError(err).Error("Server error")
		return err
	}

	s.log.Info("Shutdown telemetry server")

	return nil
}

func (s *Server) eventHandler(_ http.ResponseWriter, r *http.Request) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		s.log.WithError(err).Error("Error reading body")
	}

	var telePayload []Event
	err = jsoniter.Unmarshal(b, &telePayload)
	if err != nil {
		s.log.WithError(err).Error("Error unmarshaling telemetry request")
	}

	for _, p := range telePayload {
		if p.Type == RuntimeDone {
			s.f()
		}
	}
}

func (s *Server) Endpoint() string {
	return fmt.Sprintf("http://%s/telemetry", s.httpServer.Addr)
}
