package web

import (
	"context"
	"expvar"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/util"

	"github.com/ash2k/stager/wait"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type httpServer struct {
	logger       logrus.FieldLogger
	address      string
	Router       *mux.Router // should be private, but project layout is not great.
	rawMetricsV2 *rawHttpHandlerV2
}

type route struct {
	path    string
	handler http.HandlerFunc
	method  string
	name    string
}

var done = struct{}{}

func NewHttpServersFromViper(v *viper.Viper, logger logrus.FieldLogger, handler gostatsd.PipelineHandler) ([]*httpServer, error) {
	httpServerNames := v.GetStringSlice("http-servers")
	servers := make([]*httpServer, 0, len(httpServerNames))
	for _, httpServerName := range httpServerNames {
		server, err := newHttpServerFromViper(logger, v, httpServerName, handler)
		if err != nil {
			return nil, fmt.Errorf("failed to make http-server %s: %v", httpServerName, err)
		}
		servers = append(servers, server)
	}
	return servers, nil
}

func newHttpServerFromViper(
	logger logrus.FieldLogger,
	vMain *viper.Viper,
	serverName string,
	handler gostatsd.PipelineHandler,
) (*httpServer, error) {
	vSub := util.GetSubViper(vMain, "http."+serverName)
	vSub.SetDefault("address", "127.0.0.1:8080")
	vSub.SetDefault("enable-prof", false)
	vSub.SetDefault("enable-expvar", false)
	vSub.SetDefault("enable-ingestion", false)
	vSub.SetDefault("enable-healthcheck", true)

	return NewHttpServer(
		logger.WithField("http-server", serverName),
		handler,
		serverName,
		vSub.GetString("address"),
		vSub.GetBool("enable-prof"),
		vSub.GetBool("enable-expvar"),
		vSub.GetBool("enable-ingestion"),
		vSub.GetBool("enable-healthcheck"),
	)
}

func NewHttpServer(
	logger logrus.FieldLogger,
	handler gostatsd.PipelineHandler,
	serverName, address string,
	enableProf,
	enableExpVar,
	enableIngestion,
	enableHealthcheck bool,
) (*httpServer, error) {
	var routes []route

	server := &httpServer{
		logger:  logger,
		address: address,
	}

	if enableProf {
		profiler := &traceProfiler{}
		routes = append(routes,
			route{path: "/memprof", handler: profiler.MemProf, method: "POST", name: "profmem_post"},
			route{path: "/pprof", handler: profiler.PProf, method: "POST", name: "profpprof_post"},
			route{path: "/trace", handler: profiler.Trace, method: "POST", name: "proftrace_post"},
		)
	}

	if enableExpVar {
		routes = append(routes,
			route{path: "/expvar", handler: expvar.Handler().ServeHTTP, method: "GET", name: "expvar_get"},
		)
	}

	if enableIngestion {
		server.rawMetricsV2 = newRawHttpHandlerV2(logger, serverName, handler)
		routes = append(routes,
			route{path: "/v2/raw", handler: server.rawMetricsV2.MetricHandler, method: "POST", name: "metricsv2_post"},
			route{path: "/v2/event", handler: server.rawMetricsV2.EventHandler, method: "POST", name: "eventsv2_post"},
		)
	}

	if enableHealthcheck {
		hc := &healthChecker{logger}
		routes = append(routes,
			route{path: "/healthcheck", handler: hc.healthCheck, method: "GET", name: "healthcheck_get"},
			route{path: "/deepcheck", handler: hc.deepCheck, method: "GET", name: "deepcheck_get"},
		)
	}

	if len(routes) == 0 {
		return nil, fmt.Errorf("must enable at least one of prof, expvar, ingestion, or healthcheck")
	}

	router, err := createRoutes(routes)
	router.NotFoundHandler = server.logRequest(http.HandlerFunc(server.notFound))
	if err != nil {
		return nil, err
	}
	router.Use(server.logRequest)
	server.Router = router

	logger.WithFields(logrus.Fields{
		"address":            address,
		"enable-pprof":       enableProf,
		"enable-expvar":      enableExpVar,
		"enable-ingestion":   enableIngestion,
		"enable-healthcheck": enableHealthcheck,
	}).Info("Created server")

	return server, nil
}

func (hs *httpServer) notFound(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(404)
	_, _ = w.Write([]byte("not found"))
}

func createRoutes(routes []route) (*mux.Router, error) {
	router := mux.NewRouter()

	for _, route := range routes {
		r := router.HandleFunc(route.path, route.handler).Methods(route.method).Name(route.name)
		if err := r.GetError(); err != nil {
			return nil, fmt.Errorf("error creating route %s: %v", route.name, err)
		}
	}

	return router, nil
}

func (hs *httpServer) logRequest(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var routeName string
		route := mux.CurrentRoute(req)
		logFields := logrus.Fields{
			"route": routeName,
			"srcip": strings.Split(req.RemoteAddr, ":")[0],
			"path":  req.URL.Path,
		}
		if route == nil {
			logFields["path"] = req.URL.Path
			logFields["method"] = req.Method
		} else {
			logFields["route"] = route.GetName()
		}
		source := req.Header.Get("X-Forwarded-For")
		if source != "" {
			logFields["forwarded_for"] = source
		}

		start := time.Now()
		handler.ServeHTTP(w, req)
		dur := time.Since(start)

		logFields["duration"] = float64(dur) / float64(time.Millisecond)
		hs.logger.WithFields(logFields).Debug("request")
	})
}

func (hs *httpServer) Run(ctx context.Context) {
	if hs.rawMetricsV2 != nil {
		var wg wait.Group
		defer wg.Wait()
		wg.StartWithContext(ctx, hs.rawMetricsV2.RunMetricsContext)
	}

	server := &http.Server{
		Addr:    hs.address,
		Handler: hs.Router,
	}

	chStopped := make(chan struct{}, 1)
	go hs.waitAndStop(ctx, server, chStopped)

	hs.logger.WithField("address", server.Addr).Info("listening")

	err := server.ListenAndServe()
	if err != http.ErrServerClosed {
		hs.logger.WithError(err).Error("web server failed")
		return
	}

	// Wait for graceful shutdown of existing connections

	select {
	case <-chStopped:
		// happy
	case <-time.After(6 * time.Second):
		hs.logger.Info("timeout waiting for webserver to stop")
	}
}

// waitAndStop will gracefully shut down the Server when the Context passed is cancelled.  It signals
// on chStopped when it is done.  There is no guarantee that it will actually signal, if the server
// does not shutdown.
func (hs *httpServer) waitAndStop(ctx context.Context, server *http.Server, chStopped chan<- struct{}) {
	<-ctx.Done()

	hs.logger.Info("shutting down web server")
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := server.Shutdown(timeoutCtx)
	if err != nil {
		hs.logger.WithError(err).Warn("failed to stop web server")
	}
	chStopped <- done
}
