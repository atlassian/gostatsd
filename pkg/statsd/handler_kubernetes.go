package statsd

import (
	"context"

	"github.com/atlassian/gostatsd"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type KubernetesHandlerFactory struct {
	CacheOptions  CacheOptions
	CloudProvider gostatsd.CloudProvider // Cloud provider interface
	Limiter       *rate.Limiter
	Logger        logrus.FieldLogger
}

func (c *KubernetesHandlerFactory) New(handler gostatsd.PipelineHandler) gostatsd.PipelineHandler {
	return &KubernetesHandler{}
}

func (c *KubernetesHandlerFactory) SelfIP() (gostatsd.IP, error) {
	return c.CloudProvider.SelfIP()
}

type KubernetesHandler struct {
}

func (k *KubernetesHandler) DispatchMetrics(ctx context.Context, m []*gostatsd.Metric) {

}

func (k *KubernetesHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {

}

func (k *KubernetesHandler) EstimatedTags() int {
	return 0
}

func (k *KubernetesHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {

}

func (k *KubernetesHandler) WaitForEvents() {

}
