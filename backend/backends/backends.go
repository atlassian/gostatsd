package backends

import (
	_ "github.com/jtblin/gostatsd/backend/backends/graphite" // imports backends to avoid cycle error
	_ "github.com/jtblin/gostatsd/backend/backends/stdout"   // imports backends to avoid cycle error
)
