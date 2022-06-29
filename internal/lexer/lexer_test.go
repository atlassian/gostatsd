package lexer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/pool"
)

func TestMetricsLexer(t *testing.T) {
	t.Parallel()
	tests := map[string]gostatsd.Metric{
		"foo.bar.baz:2|c":                     {Name: "foo.bar.baz", Value: 2, Type: gostatsd.COUNTER, Rate: 1.0},
		"abc.def.g:3|g":                       {Name: "abc.def.g", Value: 3, Type: gostatsd.GAUGE, Rate: 1.0},
		"def.g:10|ms":                         {Name: "def.g", Value: 10, Type: gostatsd.TIMER, Rate: 1.0},
		"def.h:10|h":                          {Name: "def.h", Value: 10, Type: gostatsd.TIMER, Rate: 1.0},
		"def.i:10|h|#foo":                     {Name: "def.i", Value: 10, Type: gostatsd.TIMER, Rate: 1.0, Tags: gostatsd.Tags{"foo"}},
		"smp.rte:5|c|@0.1":                    {Name: "smp.rte", Value: 5, Type: gostatsd.COUNTER, Rate: 0.1},
		"smp.rte:5|c|@0.1|#foo:bar,baz":       {Name: "smp.rte", Value: 5, Type: gostatsd.COUNTER, Rate: 0.1, Tags: gostatsd.Tags{"foo:bar", "baz"}},
		"smp.rte:5|c|#foo:bar,baz":            {Name: "smp.rte", Value: 5, Type: gostatsd.COUNTER, Rate: 1.0, Tags: gostatsd.Tags{"foo:bar", "baz"}},
		"uniq.usr:joe|s":                      {Name: "uniq.usr", StringValue: "joe", Type: gostatsd.SET, Rate: 1.0},
		"fooBarBaz:2|c":                       {Name: "fooBarBaz", Value: 2, Type: gostatsd.COUNTER, Rate: 1.0},
		"smp.rte:5|c|#Foo:Bar,baz":            {Name: "smp.rte", Value: 5, Type: gostatsd.COUNTER, Rate: 1.0, Tags: gostatsd.Tags{"Foo:Bar", "baz"}},
		"smp.gge:1|g|#Foo:Bar":                {Name: "smp.gge", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"Foo:Bar"}},
		"smp.gge:1|g|#fo_o:ba-r":              {Name: "smp.gge", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"fo_o:ba-r"}},
		"smp gge:1|g":                         {Name: "smp_gge", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0},
		"smp/gge:1|g":                         {Name: "smp-gge", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0},
		"smp,gge$:1|g":                        {Name: "smpgge", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0},
		"un1qu3:john|s":                       {Name: "un1qu3", StringValue: "john", Type: gostatsd.SET, Rate: 1.0},
		"un1qu3:john|s|#some:42":              {Name: "un1qu3", StringValue: "john", Type: gostatsd.SET, Rate: 1.0, Tags: gostatsd.Tags{"some:42"}},
		"da-sh:1|s":                           {Name: "da-sh", StringValue: "1", Type: gostatsd.SET, Rate: 1.0},
		"under_score:1|s":                     {Name: "under_score", StringValue: "1", Type: gostatsd.SET, Rate: 1.0},
		"a:1|g|#f,,":                          {Name: "a", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"f"}},
		"a:1|g|#,,f":                          {Name: "a", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"f"}},
		"a:1|g|#f,,z":                         {Name: "a", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"f", "z"}},
		"a:1|g|#":                             {Name: "a", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0},
		"a:1|g|#,":                            {Name: "a", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0},
		"a:1|g|#,,":                           {Name: "a", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0},
		"foo.bar.baz:2|c|c:xyz":               {Name: "foo.bar.baz", Value: 2, Type: gostatsd.COUNTER, Rate: 1.0, Container: "xyz"},
		"smp.rte:5|c|@0.1|c:xyz":              {Name: "smp.rte", Value: 5, Type: gostatsd.COUNTER, Rate: 0.1, Container: "xyz"},
		"smp.rte:5|c|@0.1|#foo:bar,baz|c:xyz": {Name: "smp.rte", Value: 5, Type: gostatsd.COUNTER, Rate: 0.1, Tags: gostatsd.Tags{"foo:bar", "baz"}, Container: "xyz"},
		"def.i:10|h|#foo|c:xyz":               {Name: "def.i", Value: 10, Type: gostatsd.TIMER, Rate: 1.0, Tags: gostatsd.Tags{"foo"}, Container: "xyz"},
		"b:1|g|#f,,|c:xyz":                    {Name: "b", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"f"}, Container: "xyz"},
		"b:1|g|#,,f|c:xyz":                    {Name: "b", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"f"}, Container: "xyz"},
		"b:1|g|#f,,z|c:xyz":                   {Name: "b", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"f", "z"}, Container: "xyz"},
		"b:1|g|#|c:xyz":                       {Name: "b", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Container: "xyz"},
		"b:1|g|#,|c:xyz":                      {Name: "b", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Container: "xyz"},
		"b:1|g|#,,|c:xyz":                     {Name: "b", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Container: "xyz"},
		"b:1|g|#,,|c::,#@":                    {Name: "b", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Container: ":,#@"},
		"field.order.rev.all:1|g|c:xyz|#foo:bar|@0.1": {Name: "field.order.rev.all", Value: 1, Type: gostatsd.GAUGE, Rate: 0.1, Tags: gostatsd.Tags{"foo:bar"}, Container: "xyz"},
		"field.order.rev.notags:1|g|c:xyz|@0.1":       {Name: "field.order.rev.notags", Value: 1, Type: gostatsd.GAUGE, Rate: 0.1, Container: "xyz"},
		"new.field.prefix:1|g|#,,|c:xyz|x:":           {Name: "new.field.prefix", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Container: "xyz"},
		"new.field.empty:1|g|#,|c:xyz|":               {Name: "new.field.empty", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Container: "xyz"},
		"new.field.colon:1|g|#|c:xyz|:":               {Name: "new.field.colon", Value: 1, Type: gostatsd.GAUGE, Rate: 1.0, Container: "xyz"},
	}

	compareMetric(t, tests, "")
}

func TestInvalidMetricsLexer(t *testing.T) {
	t.Parallel()
	failing := []string{
		"fOO|bar:bazkk",
		"foo.bar.baz:1|q",
		"NaN.should.be:NaN|g",
		"bad.sampling:1|g|@",
		"container.missing.colon:1|c|c",
	}
	for _, tc := range failing {
		tc := tc
		t.Run(tc, func(t *testing.T) {
			t.Parallel()
			result, _, err := parseLine([]byte(tc), "")
			assert.Error(t, err, result)
		})
	}

	tests := map[string]gostatsd.Metric{
		"foo.bar.baz:2|c": {Name: "stats.foo.bar.baz", Value: 2, Type: gostatsd.COUNTER, Rate: 1.0},
		"abc.def.g:3|g":   {Name: "stats.abc.def.g", Value: 3, Type: gostatsd.GAUGE, Rate: 1.0},
		"def.g:10|ms":     {Name: "stats.def.g", Value: 10, Type: gostatsd.TIMER, Rate: 1.0},
		"uniq.usr:joe|s":  {Name: "stats.uniq.usr", StringValue: "joe", Type: gostatsd.SET, Rate: 1.0},
	}

	compareMetric(t, tests, "stats")
}

func TestEventsLexer(t *testing.T) {
	t.Parallel()
	//_e{title.length,text.length}:title|text|d:date_happened|h:hostname|p:priority|t:alert_type|#tag1,tag2
	tests := map[string]gostatsd.Event{
		"_e{1,1}:a|b":                                                     {Title: "a", Text: "b"},
		"_e{6,18}:ab|| c|hello,\\nmy friend!":                             {Title: "ab|| c", Text: "hello,\nmy friend!"},
		"_e{1,1}:a|b|d:123123":                                            {Title: "a", Text: "b", DateHappened: 123123},
		"_e{1,1}:a|b|d:123123|h:hoost":                                    {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost"},
		"_e{1,1}:a|b|d:123123|h:hoost|p:low":                              {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow},
		"_e{1,1}:a|b|d:123123|h:hoost|p:low|t:warning":                    {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning},
		"_e{1,1}:a|b|d:123123|h:hoost|p:low|t:warning|c:xyz":              {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Container: "xyz"},
		"_e{1,1}:a|b|d:123123|h:hoost|p:low|t:warning|#tag1,t:tag2":       {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|d:123123|h:hoost|p:low|t:warning|#tag1,t:tag2|c:xyz": {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}, Container: "xyz"}, //TODO is this correct?
		"_e{1,1}:a|b|t:warning|d:123123|h:hoost|p:low|c:xyz|#tag1,t:tag2": {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}, Container: "xyz"},
		"_e{1,1}:a|b|#tag1,t:tag2|t:warning|d:123123|h:hoost|p:low|c:xyz": {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}, Container: "xyz"},
		"_e{1,1}:a|b|c:xyz|#tag1,t:tag2|t:warning|d:123123|h:hoost|p:low": {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}, Container: "xyz"},
		"_e{1,1}:a|b|p:low|c:xyz|#tag1,t:tag2|t:warning|d:123123|h:hoost": {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}, Container: "xyz"},
		"_e{1,1}:a|b|h:hoost|p:low|c:xyz|#tag1,t:tag2|t:warning|d:123123": {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}, Container: "xyz"},
		"_e{1,1}:a|b|h:hoost":                                             {Title: "a", Text: "b", Source: "hoost"},
		"_e{1,1}:a|b|h:hoost|c:xyz":                                       {Title: "a", Text: "b", Source: "hoost", Container: "xyz"},
		"_e{1,1}:a|b|p:low":                                               {Title: "a", Text: "b", Priority: gostatsd.PriLow},
		"_e{1,1}:a|b|p:low|c:xyz":                                         {Title: "a", Text: "b", Priority: gostatsd.PriLow, Container: "xyz"},
		"_e{1,1}:a|b|t:warning":                                           {Title: "a", Text: "b", AlertType: gostatsd.AlertWarning},
		"_e{1,1}:a|b|t:warning|c:xyz":                                     {Title: "a", Text: "b", AlertType: gostatsd.AlertWarning, Container: "xyz"},
		"_e{1,1}:a|b|#tag1,t:tag2":                                        {Title: "a", Text: "b", Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|c:xyz":                                               {Title: "a", Text: "b", Container: "xyz"},
		"_e{20,34}:Deployment completed|Deployment completed in 7 minutes.|d:1463746133|h:9c00cf070c14|s:Micros Server|t:success|#topic:service.deploy,message_env:pdev,service_id:node-refapp-ci-internal,deployment_id:72e95b0f-37b0-4cf9-8e92-3e47d006b63f": {
			Title:          "Deployment completed",
			Text:           "Deployment completed in 7 minutes.",
			DateHappened:   1463746133,
			Source:         "9c00cf070c14",
			SourceTypeName: "Micros Server",
			Tags:           []string{"topic:service.deploy", "message_env:pdev", "service_id:node-refapp-ci-internal", "deployment_id:72e95b0f-37b0-4cf9-8e92-3e47d006b63f"},
			AlertType:      gostatsd.AlertSuccess,
		},
	}

	for input, expected := range tests {
		input := input
		expected := expected
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			_, result, err := parseLine([]byte(input), "")
			require.NoError(t, err)
			assert.Equal(t, &expected, result)
		})
	}
}

func TestInvalidEventsLexer(t *testing.T) {
	t.Parallel()
	failing := map[string]error{
		"_x{1,1}:a|b":                        errInvalidType,
		"_e{2,1}:a|b":                        errNotEnoughData,
		"_e{1,2}:a|b":                        errNotEnoughData,
		"_e{2,2}:a|b":                        errNotEnoughData,
		"_e{1,1}:ab":                         errNotEnoughData,
		"_e{1,1}ab":                          errInvalidFormat,
		"_e{1,1}a:b":                         errInvalidFormat,
		"_e{1,1}:a:b":                        errInvalidFormat,
		"_e{,1}:a|b":                         errInvalidFormat,
		"_e{1,}:a|b":                         errInvalidFormat,
		"_e{1}:a|b":                          errInvalidFormat,
		"_e{}:a|b":                           errInvalidFormat,
		"_e1,2}:a|b":                         errInvalidFormat,
		"_e:a|b":                             errInvalidFormat,
		"_e{999999999999999999999999,1}:a|b": errOverflow,
		"_e{1,999999999999999999999999}:a|b": errOverflow,
	}
	for input, expectedErr := range failing {
		input := input
		expectedErr := expectedErr
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			m, e, err := parseLine([]byte(input), "")
			assert.Equal(t, expectedErr, err)
			assert.Nil(t, m)
			assert.Nil(t, e)
		})
	}
}

func parseLine(input []byte, namespace string) (*gostatsd.Metric, *gostatsd.Event, error) {
	l := Lexer{
		MetricPool: pool.NewMetricPool(0),
	}
	return l.Run(input, namespace)
}

func compareMetric(t *testing.T, tests map[string]gostatsd.Metric, namespace string) {
	for input, expected := range tests {
		input := input
		expected := expected
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			result, _, err := parseLine([]byte(input), namespace)
			result.DoneFunc = nil // Clear DoneFunc because it contains non-predictable variable data which interferes with the tests
			require.NoError(t, err)
			assert.Equal(t, &expected, result)
		})
	}
}

var parselineBlackhole *gostatsd.Metric

func benchmarkLexer(ns string, input string, b *testing.B) {
	slice := []byte(input)
	var r *gostatsd.Metric

	l := &Lexer{
		MetricPool: pool.NewMetricPool(0),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		r, _, _ = l.Run(slice, ns)
		r.Done()
	}
	parselineBlackhole = r
}

func BenchmarkParseCounter(b *testing.B) {
	benchmarkLexer("", "foo.bar.baz:2|c", b)
}
func BenchmarkParseCounterWithSampleRate(b *testing.B) {
	benchmarkLexer("", "smp.rte:5|c|@0.1", b)
}
func BenchmarkParseCounterWithTags(b *testing.B) {
	benchmarkLexer("", "smp.rte:5|c|#foo:bar,baz", b)
}
func BenchmarkParseCounterWithTagsAndSampleRate(b *testing.B) {
	benchmarkLexer("", "smp.rte:5|c|@0.1|#foo:bar,baz", b)
}
func BenchmarkParseGauge(b *testing.B) {
	benchmarkLexer("", "abc.def.g:3|g", b)
}
func BenchmarkParseTimer(b *testing.B) {
	benchmarkLexer("", "def.g:10|ms", b)
}
func BenchmarkParseSet(b *testing.B) {
	benchmarkLexer("", "uniq.usr:joe|s", b)
}
func BenchmarkParseCounterWithDefaultTags(b *testing.B) {
	benchmarkLexer("", "foo.bar.baz:2|c", b)
}
func BenchmarkParseCounterWithDefaultTagsAndTags(b *testing.B) {
	benchmarkLexer("", "foo.bar.baz:2|c|#foo:bar,baz", b)
}
func BenchmarkParseCounterWithDefaultTagsAndTagsAndNameSpace(b *testing.B) {
	benchmarkLexer("stats", "foo.bar.baz:2|c|#foo:bar,baz", b)
}
