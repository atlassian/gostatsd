package statsd

import (
	"fmt"
	"html/template"
	"net/http"
)

// DefaultWebConsoleAddr is the default address on which a WebConsoleServer will listen.
const DefaultWebConsoleAddr = ":8181"

// WebConsoleServer is an object that listens for HTTP connection on a TCP address Addr
// and provides a web interface for its MetricAggregator.
type WebConsoleServer struct {
	Addr       string
	Aggregator *MetricAggregator
}

const tempText = `
<html>
<head>
<title>gostatsd</title>
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css" integrity="sha384-1q8mTJOASx8j1Au+a5WDVnPi2lkFfwwEAa8hDDdjZlpLegxhjVME1fgjWPGmkzs7" crossorigin="anonymous">
<style>
.table {
	font-size: 14px;
}
</style>
</head>
<body>
<div class="container">
<div class="row">
<div class="col-md9">
  	<h1 class="page-header">gostatsd</h1>
	<p>Bad lines received: {{.Stats.BadLines}}</p>
	<p>Last messsage received: {{.Stats.LastMessage}}</p>
	<p>Last flush to backends: {{.Stats.LastFlush}}</p>
	<p>Last error flushing to backends: {{.Stats.LastFlushError}}</p>
	<h2>Counters</h2>
	<table class="table">
	<thead>
	<tr><th>Name</th><th>Rate</th><th>Tags</th></tr>
	</thead>
	{{range $metric, $value := .Counters}}
	{{range $tags, $counter := $value}}
	<tr><td>{{$metric}}</td><td>{{$counter.PerSecond}}</td><td>{{$tags}}</td></tr>
	{{end}}
	{{end}}
	</table>
	<h2>Gauges</h2>
	<table class="table">
	<thead>
	<tr><th>Name</th><th>Value</th><th>Tags</th></tr>
	</thead>
	{{range $metric, $value := .Gauges}}
	{{range $tags, $gauge := $value}}
	<tr><td>{{$metric}}</td><td>{{$gauge.Value}}</td><td>{{$tags}}</td></tr>
	{{end}}
	{{end}}
	</table>
	<h2>Timers</h2>
	<table class="table">
	<thead>
	<tr><th>Name</th><th>Value</th><th>Tags</th></tr>
	</thead>
	{{range $metric, $value := .Timers}}
	{{range $tags, $timer := $value}}
	{{range $idx, $time := $timer.Values}}
	<tr><td>{{$metric}}.{{$idx}}</td><td>{{$time}}</td><td>{{$tags}}</td></tr>
	{{end}}
	{{end}}
	{{end}}
	</table>
	<h2>Sets</h2>
	<table class="table">
	<thead>
	<tr><th>Name</th><th>Count</th><th>Tags</th></tr>
	</thead>
	{{range $metric, $value := .Sets}}
	{{range $tags, $set := $value}}
	<tr><td>{{$metric}}</td><td>{{size $set.Values}}</td><td>{{$tags}}</td></tr>
	{{end}}
	{{end}}
	</table>
</div>
</div>
</div>
</body>
</html>
`

var funcMap = template.FuncMap{"size": size}
var temp = template.Must(template.New("temp").Funcs(funcMap).Parse(tempText))

func size(arr map[string]int64) string {
	return fmt.Sprintf("%d", len(arr))
}

func (s *WebConsoleServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.Aggregator.Lock()
	defer s.Aggregator.Unlock()
	err := temp.Execute(w, s.Aggregator)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ListenAndServe listens on the ConsoleServer's TCP network address and then calls Serve.
func (s *WebConsoleServer) ListenAndServe() error {
	if s.Addr == "" {
		s.Addr = DefaultWebConsoleAddr
	}
	return http.ListenAndServe(s.Addr, s)
}
