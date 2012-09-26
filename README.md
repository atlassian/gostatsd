gostatsd
========

An implementation of [Etsy's][etsy] [statsd][statsd] in Go.

The project provides both a server called "gostatsd" which works much like
Etsy's version, but also provides a library for developing customized servers.


Building the server
-------------------
From the `gostatsd/` directory run `go build`. The binary will be built in place
and called `gostatsd`

Using the library
-----------------
In your source code:

    import "github.com/kisielk/gostatsd/statsd"

Documentation can be found via `go doc github.com/kisielk/gostatsd/statsd` or at
http://go.pkgdoc.org/github.com/kisielk/gostatsd/statsd

[etsy]: http://www.etsy.com
[statsd]: http://www.github.com/etsy/statsd
