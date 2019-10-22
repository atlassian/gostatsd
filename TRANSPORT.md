Configurable transports
-----------------------
Almost all the http clients (AWS Cloud Provider is currently excluded) used throughout the service are configurable
in a uniform manner.  Various components will take a `transport` setting to select which named client to use.  Each
client is configured in the following manner:

```
[transport.<name>]
client-timeout = '10s'
compress = true
custom-headers = {}
debug-body = false
max-parallel-requests = 1000
type = 'http'
user-agent = "gostatsd"

retry-* = ... see section on retries below ...
```

- `client-timeout`: The maximum time for a single roundtrip to execute. Set to `0` to disable timeout.  Retries
  will allow the total time to run for longer than this value.  Corresponds to `http.Client.Timeout`.
- `compress`: Indicates if data should be compressed if possible (some backends don't support compression)
- `custom-headers`: Allows for custom headers to be set.  See `HTTP Headers` section below for further information.
- `debug-body`: Attempts to serialize a request in a more human friendly format.  For protobuf this means text, and
  for JSON this means with new lines and indentation.  Disables compression.  Warning: Text encoded protobuf will not
  typically be accepted by servers.
- `max-parallel-requests`: The maximum number of requests in flight on this transport.  This is network only, and
  doesn't include CPU bound work (serialization and compression).
- `type`: There is currently only a type of `http`, however others are planned (Kinesis, Kafka, etc).  Each type
  has additional configuration which is included in the `transport.<name>` stanza.
- `user-agent`: A user-agent header to attach to all requests.

If a transport is not configured, it will fallback to the transport named `default` (ie, `transport.default`) and
emit a warning message.

Retry configuration
-------------------
All the retry options are listed below, not all options apply to all policies.
- `retry-policy`: the retry policy to apply, one of `exponential` (default), `constant`, and `disabled`.
- `retry-interval`: the interval between retries, applies only to the `constant` policy.  Default value is `1s`, must
  be positive.
- `retry-max-count`: the maximum number of retries to apply.  Applies to `exponential` and `constant` policies.  A
  value of `0` means unlimited.  Must be zero or positive.  Default value is `0` (unlimited)
- `retry-max-time`: the maximum amount of time to try applying retries.  Applies to `exponential` and `constant`
  policies.  Must be positive.  Default value is `15s`.

The actual delay between individual retries will be +/- 0.5 * interval, where the interval depends on the policy.

Retry examples
--------------
```
# No retries
[transport.no-retries]
retry-policy = 'disabled'

# Retry "forever" with a 10 second retry interval
[transport.retry-forever-constant]
retry-policy = 'constant'
retry-interval = '10s'
retry-max-time = '1y'  # there's no actual "forever"

# Retry forever with an exponential backoff
[transport.retry-forever-exponential]
retry-policy = 'exponential'
retry-max-time = '1y'  # there's no actual "forever"

# Retry a maximum of 5 times (6 actual attempts)
[transport.retry-max-count]
retry-policy = 'constant'
retry-interval = '1s'
retry-duration = '1m'  # Will not be reached, retry-max-count will take priority
retry-max-count = 5
```

HTTP headers
------------
There are 3 layers of headers which are on every request.  They are processed in the following order:
1. Base headers.  This is the `user-agent`, `encoding`, and `content-type`.
2. Backend specific headers.  This covers headers for API keys, such as `X-Insert-Key` for NewRelic.
3. User headers.  These always take precedence, and setting a value of `""` allows for a header set at an earlier
   layer to be removed.

Custom headers can be specified as:
```
custom-headers={"header1"="value1", "header2"="value2"}
```

or

```
custom-headers.header1="value1"
custom-headers.header2="value2"
```

HTTP transport configuration
----------------------------
All TLS connections requires at least TLS 1.2, and the proxy is taken from the [environment](https://golang.org/pkg/net/http/#ProxyFromEnvironment).

All settings and defaults:
```
[transport.<name>]
client-timeout = '10s'
type = 'http'
dialer-keep-alive = '30s'
dialer-timeout = '5s'
enable-http2 = false
idle-connection-timeout = '1m'
max-idle-connections = 50
network = 'tcp'
tls-handshake-timeout = '3m'
```

- `dialer-keep-alive`: The network level keep-alive, if supported.  This is typically TCP level, and is not HTTP
  level.  Set to `-1` to disable, `0` to enable, and above `0` to specify an interval.
  Corresponds to `net.Dialer#KeepAlive`.
- `dialer-timeout`: The total time to resolve and connect to the remote server.  Set to `0` to disable timeout,
  however the OS may impose its own limit.  This duration may be split across each IP in the DNS.
  Corresponds to `net.Dialer#Timeout`.
- `enable-http2`: Enables or disables HTTP2 support.  There seems to be some incompatibility with the golang http2
  implementation and AWS ELB/ALBs.  If you experience strange timeouts and hangs, this should be the first thing
  to disable.
- `idle-connection-timeout`: How long before idle connections are closed.  Set to `0` to disable timeout, must not
  be negative.
  Corresponds to `http.Transport#IdleConnTimeout`.
- `max-idle-connections`: Maximum number of idle connections.  Set to `0` for unlimited, must not be negative.
  Corresponds to `http.Transport#MaxIdleConns`.
- `network`: Set the protocol to use.  May be anything accepted by a `net.Dialer`, common values are `tcp`, `tcp4`, and
  `tcp6`.
  Corresponds to the `net.Dialer#Dial` `network` parameter.
- `tls-handshake-timeout`: The maximum amount of time waiting for a TLS handshake.  Set to `0` to disable timeout, must
  not be negative.
  Corresponds to `http.Transport#TLSHandshakeTimeout`.
- `response-header-timeout`: If non-zero, specifies the amount of time to wait for a server's response headers after
  fully writing the request (including its body, if any). It time does not include the time to read the response body.
  Defaults to zero.
  Corresponds to `http.Transport#ResponseHeaderTimeout`.
