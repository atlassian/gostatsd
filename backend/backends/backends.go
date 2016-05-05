package backends

import (
	_ "github.com/atlassian/gostatsd/backend/backends/datadog"     // imports backends to avoid cycle error
	_ "github.com/atlassian/gostatsd/backend/backends/graphite"    // imports backends to avoid cycle error
	_ "github.com/atlassian/gostatsd/backend/backends/null"        // imports backends to avoid cycle error
	_ "github.com/atlassian/gostatsd/backend/backends/statsdaemon" // imports backends to avoid cycle error
	_ "github.com/atlassian/gostatsd/backend/backends/stdout"      // imports backends to avoid cycle error
)
