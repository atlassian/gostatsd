package statsd

import (
	"html/template"
	"net/http"
)

// WebConsoleServer is an object that listens for HTTP connection on a TCP address Addr
// and provides a web interface for its MetricAggregator
type WebConsoleServer struct {
	Addr       string
	Aggregator *MetricAggregator
}

const tempText = `
<html>
<head>
<title>gostatsd</title>
<style>
table
{
border-collapse:collapse;
}
table, th, td {
	border: 1px solid black;
}
</style>
</head>
<body>
<h1>gostatsd</h1>
<p>Bad lines received: {{.Stats.BadLines}}</p>
<p>Last messsage received: {{.Stats.LastMessage}}</p>
<p>Last flush to backends: {{.Stats.LastFlush}}</p>
<p>Last error flushing to backends: {{.Stats.LastFlushError}}</p>
<h2>Counters</h2>
<table>
<tr><th>Bucket</th><th>Value</th></tr>
{{range $bucket, $value := .Counters}}
<tr><td>{{$bucket}}</td><td>{{$value}}</td></tr>
{{end}}
</table>
<h2>Gauges</h2>
<table>
<tr><th>Bucket</th><th>Value</th></tr>
{{range $bucket, $value := .Gauges}}
<tr><td>{{$bucket}}</td><td>{{$value}}</td></tr>
{{end}}
</table>
<h2>Timers</h2>
<table>
<tr><th>Bucket</th><th>Value</th></tr>
{{range $bucket, $value := .Timers}}
<tr><td>{{$bucket}}</td><td>{{$value}}</td></tr>
{{end}}
</table>
</body>
</html>
`

var temp = template.Must(template.New("temp").Parse(tempText))

func (s *WebConsoleServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	defer s.Aggregator.Unlock()
	s.Aggregator.Lock()
	err := temp.Execute(w, s.Aggregator)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ListenAndServe listens on the ConsoleServer's TCP network address and then calls Serve
func (s *WebConsoleServer) ListenAndServe() error {
	if s.Addr == "" {
		s.Addr = DefaultConsoleAddr
	}
	return http.ListenAndServe(s.Addr, s)
}
