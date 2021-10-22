package api_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/atlassian/gostatsd/internal/awslambda/extension/api"
)

func TestExpectedURL(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		domain string
		path   api.LambdaPath
		expect string
	}{
		{domain: "localhost", path: api.EventEndpoint, expect: "http://localhost/2020-01-01/extension/event/next"},
		{domain: "localhost/", path: api.ExitErrorEndpoint, expect: "http://localhost/2020-01-01/extension/exit/error"},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expect, tc.path.GetUrl(tc.domain))
	}
}
