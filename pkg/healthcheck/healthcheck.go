package healthcheck

// HealthcheckFunc is a function that returns a status message, and if the check if healthy or not (false).
// healthchecks must not block, and downstream dependencies should be reported on via a watchdog style, and not by
// making a roundtrip.
type HealthcheckFunc func() (string, HealthyStatus)

type HealthyStatus bool

const (
	Healthy   = HealthyStatus(true)
	Unhealthy = HealthyStatus(false)
)

type HealthCheckProvider interface {
	HealthChecks() []HealthcheckFunc
}

type DeepCheckProvider interface {
	DeepChecks() []HealthcheckFunc
}

func MaybeAppendHealthChecks(healthChecks []HealthcheckFunc, deepChecks []HealthcheckFunc, maybeProvider interface{}) ([]HealthcheckFunc, []HealthcheckFunc) {
	if hcp, ok := maybeProvider.(HealthCheckProvider); ok {
		healthChecks = append(healthChecks, hcp.HealthChecks()...)
	}
	if dcp, ok := maybeProvider.(DeepCheckProvider); ok {
		deepChecks = append(deepChecks, dcp.DeepChecks()...)
	}
	return healthChecks, deepChecks
}
