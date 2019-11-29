package web

import (
	"net/http"

	"github.com/sirupsen/logrus"
)

type healthChecker struct {
	logger logrus.FieldLogger
}

// healthCheck reports if the server is ready to process traffic.  It does not validate downstream dependencies.
func (hc *healthChecker) healthCheck(w http.ResponseWriter, req *http.Request) {
	hc.logger.Info("healthCheck")
	_, _ = w.Write([]byte("OK"))
}

// deepCheck reports on the status of downstream dependencies.  It should be non-blocking (ie, report on the status
// of watchdogs, rather than make roundtrips).  For now, it just reports that it's always happy.
func (hc *healthChecker) deepCheck(w http.ResponseWriter, req *http.Request) {
	hc.logger.Info("deepCheck")
	_, _ = w.Write([]byte("OK"))
}
