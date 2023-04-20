package web

import (
	"net/http"

	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"

	"github.com/atlassian/gostatsd/pkg/healthcheck"
)

type healthChecker struct {
	logger       logrus.FieldLogger
	healthChecks []healthcheck.HealthcheckFunc
	deepChecks   []healthcheck.HealthcheckFunc
}

func runHealthChecks(checks []healthcheck.HealthcheckFunc) (good []string, bad []string) {
	// Force it render as an array, not null
	good = []string{}
	bad = []string{}
	for _, check := range checks {
		report, isHealthy := check()
		if isHealthy == healthcheck.Healthy {
			good = append(good, report)
		} else {
			bad = append(bad, report)
		}
	}
	return good, bad
}

func respondToHealthChecks(resp http.ResponseWriter, checks []healthcheck.HealthcheckFunc) {
	good, bad := runHealthChecks(checks)
	resp.Header().Set("content-type", "application/json")
	if len(bad) > 0 {
		resp.WriteHeader(http.StatusInternalServerError)
	} else {
		resp.WriteHeader(http.StatusOK)
	}

	enc := jsoniter.NewEncoder(resp)
	_ = enc.Encode(map[string][]string{
		"ok":     good,
		"failed": bad,
	})
}

// healthCheck reports if the server is ready to process traffic.
func (hc *healthChecker) healthCheck(resp http.ResponseWriter, req *http.Request) {
	hc.logger.Info("healthCheck")
	respondToHealthChecks(resp, hc.healthChecks)
}

// deepCheck reports on the status of downstream dependencies.
func (hc *healthChecker) deepCheck(resp http.ResponseWriter, req *http.Request) {
	hc.logger.Info("deepCheck")
	respondToHealthChecks(resp, hc.deepChecks)
}
