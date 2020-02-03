package statsd

import "github.com/atlassian/gostatsd"

var (
	_ gostatsd.CloudHandlerFactory = &KubernetesHandlerFactory{}
	_ gostatsd.PipelineHandler     = &KubernetesHandler{}
)
