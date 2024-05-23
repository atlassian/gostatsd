package lambda

import (
	"github.com/sirupsen/logrus"

	"github.com/atlassian/gostatsd/internal/awslambda/extension"
	"github.com/atlassian/gostatsd/internal/flush"
	"github.com/atlassian/gostatsd/pkg/statsd"
)

// Extension exposes the internal type definition.
//
// Note: This is not ideal but simplifies exposing the API.
type Extension = extension.Server

// NewExtension extends a `statsd.Server` by adding a server that integrates with
// the AWS Lambda Extension Runtime API to allow near native support for on host collection.
// The provided server is then run within the extension in additional to the server for AWS integration.
func NewExtension(logger logrus.FieldLogger, server *statsd.Server, opts Options) (Extension, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	s := *server

	var extOpts []extension.ManagerOpt
	if opts.EnableManualFlush {
		fc := flush.NewFlushCoordinator()
		s.ForwarderFlushCoordinator = fc
		extOpts = append(extOpts, extension.WithManualFlushEnabled(fc, opts.TelemetryAddr))
	}

	return extension.NewManager(
		opts.RuntimeAPI,
		opts.ExecutableName,
		logger,
		&s,
		extOpts...,
	), nil
}
