## HTTP endpoints

### `pprof` endpoints
- `/debug/pprof/trace`, runs a trace profile for 30 seconds
- `/debug/pprof/profile`, runs a pprof profile for 30 seconds
- `/debug/pprof/heap`, runs a `heap` memory profile for 30 seconds

Only one prof will be allowed to run at any point, and requesting multiple will block until the previous has completed.

### `expvar` endpoints
- `/expvar`, routes directly to the [expvar handler](https://golang.org/pkg/expvar/#Handler)

### `healthcheck` endpoints
- `/healthcheck`, reports if the server is internally healthy.  This is what should be used for health checking by a
  load-balancer.  It does not report on downstream dependencies.
- `/deepcheck`, reports the status of downstream dependencies.  This should not be used for system healthcheck, as a bad
  dependency should not cause an otherwise healthy server to cycle, because it will likely fail again.  It is generally
  checked on startup, not continuously, but is designed with the assumption that it will be continuously polled.

Both endpoints will return 200 if healthy, or 500 if not healthy.  Currently the body is:
```
{
  "ok": ["results of checks"],
  "failed": ["results of checks"]
}
```

however this is not contractual and may change.  The contract is the error code, not the body.  A 500 is returned if
the `failed` field is non-empty.

### `ingestion` endpoint
- `/vN/raw` and `/vN/event`, takes in protobuf formatted raw metrics.  This endpoint is intended for gostatsd to
  gostatsd communication only, and thus not documented. This is to deter a service which may not bother to consolidate
  metrics, which would be detrimental to the health of the aggregation service. The version spoken by a forwarding
  instance is not configurable.

  The compatibility guarantee is:
  - A bump of N will be a major bump of the service version
  - A bump of N will retain support for N-1, dropping N-2 if present.
  - A major bump of the service version may not increase N.
  - A major bump of the service version may drop N-1 support without adding N+1.
  - There will never be more than N-1 and N.

  All changes of N will be documented in the [CHANGELOG.md](CHANGELOG.md).  N is currently 2.
