package statsd

import (
	"reflect"
	"testing"

	"github.com/atlassian/gostatsd/types"
)

func TestMetricsLexer(t *testing.T) {
	tests := map[string]types.Metric{
		"foo.bar.baz:2|c":               {Name: "foo.bar.baz", Value: 2, Type: types.COUNTER},
		"abc.def.g:3|g":                 {Name: "abc.def.g", Value: 3, Type: types.GAUGE},
		"def.g:10|ms":                   {Name: "def.g", Value: 10, Type: types.TIMER},
		"smp.rte:5|c|@0.1":              {Name: "smp.rte", Value: 50, Type: types.COUNTER},
		"smp.rte:5|c|@0.1|#foo:bar,baz": {Name: "smp.rte", Value: 50, Type: types.COUNTER, Tags: types.Tags{"foo:bar", "baz"}},
		"smp.rte:5|c|#foo:bar,baz":      {Name: "smp.rte", Value: 5, Type: types.COUNTER, Tags: types.Tags{"foo:bar", "baz"}},
		"uniq.usr:joe|s":                {Name: "uniq.usr", StringValue: "joe", Type: types.SET},
		"fooBarBaz:2|c":                 {Name: "fooBarBaz", Value: 2, Type: types.COUNTER},
		"smp.rte:5|c|#Foo:Bar,baz":      {Name: "smp.rte", Value: 5, Type: types.COUNTER, Tags: types.Tags{"foo:bar", "baz"}},
		"smp.gge:1|g|#Foo:Bar":          {Name: "smp.gge", Value: 1, Type: types.GAUGE, Tags: types.Tags{"foo:bar"}},
		"smp.gge:1|g|#fo_o:ba-r":        {Name: "smp.gge", Value: 1, Type: types.GAUGE, Tags: types.Tags{"fo_o:ba-r"}},
		"smp gge:1|g":                   {Name: "smp_gge", Value: 1, Type: types.GAUGE},
		"smp/gge:1|g":                   {Name: "smp-gge", Value: 1, Type: types.GAUGE},
		"smp,gge$:1|g":                  {Name: "smpgge", Value: 1, Type: types.GAUGE},
		"un1qu3:john|s":                 {Name: "un1qu3", StringValue: "john", Type: types.SET},
		"un1qu3:john|s|#some:42":        {Name: "un1qu3", StringValue: "john", Type: types.SET, Tags: types.Tags{"some:42"}},
		"da-sh:1|s":                     {Name: "da-sh", StringValue: "1", Type: types.SET},
		"under_score:1|s":               {Name: "under_score", StringValue: "1", Type: types.SET},
	}

	compareMetric(tests, "", t)
}

func TestInvalidMetricsLexer(t *testing.T) {
	failing := []string{"fOO|bar:bazkk", "foo.bar.baz:1|q", "NaN.should.be:NaN|g"}
	for _, tc := range failing {
		result, _, err := parseLine([]byte(tc), "")
		if err == nil {
			t.Errorf("test %s: expected error but got %s", tc, result)
		}
	}

	tests := map[string]types.Metric{
		"foo.bar.baz:2|c": {Name: "stats.foo.bar.baz", Value: 2, Type: types.COUNTER},
		"abc.def.g:3|g":   {Name: "stats.abc.def.g", Value: 3, Type: types.GAUGE},
		"def.g:10|ms":     {Name: "stats.def.g", Value: 10, Type: types.TIMER},
		"uniq.usr:joe|s":  {Name: "stats.uniq.usr", StringValue: "joe", Type: types.SET},
	}

	compareMetric(tests, "stats", t)
}

func TestEventsLexer(t *testing.T) {
	//_e{title.length,text.length}:title|text|d:date_happened|h:hostname|p:priority|t:alert_type|#tag1,tag2
	tests := map[string]types.Event{
		"_e{1,1}:a|b":                                               {Title: "a", Text: "b"},
		"_e{6,18}:ab|| c|hello,\\nmy friend!":                       {Title: "ab|| c", Text: "hello,\nmy friend!"},
		"_e{1,1}:a|b|d:123123":                                      {Title: "a", Text: "b", DateHappened: 123123},
		"_e{1,1}:a|b|d:123123|h:hoost":                              {Title: "a", Text: "b", DateHappened: 123123, Hostname: "hoost"},
		"_e{1,1}:a|b|d:123123|h:hoost|p:low":                        {Title: "a", Text: "b", DateHappened: 123123, Hostname: "hoost", Priority: types.PriLow},
		"_e{1,1}:a|b|d:123123|h:hoost|p:low|t:warning":              {Title: "a", Text: "b", DateHappened: 123123, Hostname: "hoost", Priority: types.PriLow, AlertType: types.AlertWarning},
		"_e{1,1}:a|b|d:123123|h:hoost|p:low|t:warning|#tag1,t:tag2": {Title: "a", Text: "b", DateHappened: 123123, Hostname: "hoost", Priority: types.PriLow, AlertType: types.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|t:warning|d:123123|h:hoost|p:low|#tag1,t:tag2": {Title: "a", Text: "b", DateHappened: 123123, Hostname: "hoost", Priority: types.PriLow, AlertType: types.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|p:low|t:warning|d:123123|h:hoost|#tag1,t:tag2": {Title: "a", Text: "b", DateHappened: 123123, Hostname: "hoost", Priority: types.PriLow, AlertType: types.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|h:hoost|p:low|t:warning|d:123123|#tag1,t:tag2": {Title: "a", Text: "b", DateHappened: 123123, Hostname: "hoost", Priority: types.PriLow, AlertType: types.AlertWarning, Tags: []string{"tag1", "t:tag2"}},

		"_e{1,1}:a|b|h:hoost":      {Title: "a", Text: "b", Hostname: "hoost"},
		"_e{1,1}:a|b|p:low":        {Title: "a", Text: "b", Priority: types.PriLow},
		"_e{1,1}:a|b|t:warning":    {Title: "a", Text: "b", AlertType: types.AlertWarning},
		"_e{1,1}:a|b|#tag1,t:tag2": {Title: "a", Text: "b", Tags: []string{"tag1", "t:tag2"}},
		"_e{20,34}:Deployment completed|Deployment completed in 7 minutes.|d:1463746133|h:9c00cf070c14|s:Micros Server|t:success|#topic:service.deploy,message_env:pdev,service_id:node-refapp-ci-internal,deployment_id:72e95b0f-37b0-4cf9-8e92-3e47d006b63f": {
			Title:          "Deployment completed",
			Text:           "Deployment completed in 7 minutes.",
			DateHappened:   1463746133,
			Hostname:       "9c00cf070c14",
			SourceTypeName: "Micros Server",
			Tags:           []string{"topic:service.deploy", "message_env:pdev", "service_id:node-refapp-ci-internal", "deployment_id:72e95b0f-37b0-4cf9-8e92-3e47d006b63f"},
			AlertType:      types.AlertSuccess,
		},
	}

	compareEvent(tests, t)
}

func TestInvalidEventsLexer(t *testing.T) {
	failing := map[string]error{
		"_x{1,1}:a|b": errInvalidType,
		"_e{2,1}:a|b": errNotEnoughData,
		"_e{1,2}:a|b": errNotEnoughData,
		"_e{2,2}:a|b": errNotEnoughData,
		"_e{1,1}:ab":  errNotEnoughData,
		"_e{1,1}ab":   errInvalidFormat,
		"_e{1,1}a:b":  errInvalidFormat,
		"_e{1,1}:a:b": errInvalidFormat,
		"_e{,1}:a|b":  errInvalidFormat,
		"_e{1,}:a|b":  errInvalidFormat,
		"_e{1}:a|b":   errInvalidFormat,
		"_e{}:a|b":    errInvalidFormat,
		"_e1,2}:a|b":  errInvalidFormat,
		"_e:a|b":      errInvalidFormat,
		"_e{999999999999999999999999,1}:a|b": errOverflow,
		"_e{1,999999999999999999999999}:a|b": errOverflow,
	}
	for input, expectedErr := range failing {
		m, e, err := parseLine([]byte(input), "")
		if m != nil || e != nil || !reflect.DeepEqual(err, expectedErr) {
			t.Errorf("test %s: expected error %q but got %q, %q and %q", input, expectedErr, m, e, err)
		}
	}
}

func parseLine(input []byte, namespace string) (*types.Metric, *types.Event, error) {
	l := lexer{}
	return l.run(input, namespace)
}

func compareMetric(tests map[string]types.Metric, namespace string, t *testing.T) {
	for input, expected := range tests {
		result, _, err := parseLine([]byte(input), namespace)
		if err != nil {
			t.Errorf("test %s error: %v", input, err)
			continue
		}
		if !reflect.DeepEqual(result, &expected) {
			t.Errorf("test %s: expected %s, got %s", input, expected, result)
		}
	}
}

func compareEvent(tests map[string]types.Event, t *testing.T) {
	for input, expected := range tests {
		_, result, err := parseLine([]byte(input), "")
		if err != nil {
			t.Errorf("test %s error: %v", input, err)
			continue
		}
		if !reflect.DeepEqual(result, &expected) {
			t.Errorf("test %s: expected %s, got %s", input, expected, result)
		}
	}
}

var parselineBlackhole *types.Metric

func benchmarkLexer(mr *metricReceiver, input string, b *testing.B) {
	slice := []byte(input)
	var r *types.Metric
	for n := 0; n < b.N; n++ {
		r, _, _ = mr.parseLine(slice)
	}
	parselineBlackhole = r
}

func BenchmarkParseCounter(b *testing.B) {
	benchmarkLexer(&metricReceiver{}, "foo.bar.baz:2|c", b)
}
func BenchmarkParseCounterWithSampleRate(b *testing.B) {
	benchmarkLexer(&metricReceiver{}, "smp.rte:5|c|@0.1", b)
}
func BenchmarkParseCounterWithTags(b *testing.B) {
	benchmarkLexer(&metricReceiver{}, "smp.rte:5|c|#foo:bar,baz", b)
}
func BenchmarkParseCounterWithTagsAndSampleRate(b *testing.B) {
	benchmarkLexer(&metricReceiver{}, "smp.rte:5|c|@0.1|#foo:bar,baz", b)
}
func BenchmarkParseGauge(b *testing.B) {
	benchmarkLexer(&metricReceiver{}, "abc.def.g:3|g", b)
}
func BenchmarkParseTimer(b *testing.B) {
	benchmarkLexer(&metricReceiver{}, "def.g:10|ms", b)
}
func BenchmarkParseSet(b *testing.B) {
	benchmarkLexer(&metricReceiver{}, "uniq.usr:joe|s", b)
}
func BenchmarkParseCounterWithDefaultTags(b *testing.B) {
	benchmarkLexer(&metricReceiver{}, "foo.bar.baz:2|c", b)
}
func BenchmarkParseCounterWithDefaultTagsAndTags(b *testing.B) {
	benchmarkLexer(&metricReceiver{}, "foo.bar.baz:2|c|#foo:bar,baz", b)
}
func BenchmarkParseCounterWithDefaultTagsAndTagsAndNameSpace(b *testing.B) {
	benchmarkLexer(&metricReceiver{namespace: "stats"}, "foo.bar.baz:2|c|#foo:bar,baz", b)
}
