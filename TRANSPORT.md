Configurable transports
-----------------------
Almost all the http clients (AWS Cloud Provider is currently excluded) used throughout the service are configurable
in a uniform manner.  Various components will take a `transport` setting to select which named client to use.  Each
client is configured in the following manner:

```
[transport.<name>]
client-timeout = '10s'
type = 'http'
```

- `client-timeout`: The maximum time for the roundtrip to execute. Set to `0` to disable timeout.
  Corresponds to `http.Client.Timeout`.
- `type`: There is currently only a type of `http`, however others are planned (Kinesis, Kafka, etc).  Each type
  has additional configuration which is included in the `transport.<name>` stanza.

If a transport is not configured, it will fallback to the transport named `default` (ie, `transport.default`) and
emit a warning message.

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
