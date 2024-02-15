//go:build fips

package main

// To ensure that fips is only built when requested,
// the go build tags are used to adopt the allowed crypto libraries.

import _ "crypto/tls/fipsonly"
