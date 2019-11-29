package transport

import (
	"net/http"
)

// Client is a holder of an http.Client.  In future it will have some high level logic
// attached to it (PostProtobuf, PostJson, retries, metrics, etc).  The underlying Client
// is exposed so that things that require a real http.Client (such as Cloudwatch) can
// still utilize the TransportPool.
type Client struct {
	Client *http.Client
}
