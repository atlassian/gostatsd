package lexer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/pool"
)

func TestMetricsLexer(t *testing.T) {
	t.Parallel()
	tests := map[string]gostatsd.Metric{
		"foo.bar.baz:2|c":                             {Name: "foo.bar.baz", Values: []float64{2}, Type: gostatsd.COUNTER, Rate: 1.0},
		"abc.def.g:3|g":                               {Name: "abc.def.g", Values: []float64{3}, Type: gostatsd.GAUGE, Rate: 1.0},
		"def.g:10|ms":                                 {Name: "def.g", Values: []float64{10}, Type: gostatsd.TIMER, Rate: 1.0},
		"def.h:10|h":                                  {Name: "def.h", Values: []float64{10}, Type: gostatsd.TIMER, Rate: 1.0},
		"def.i:10|h|#foo":                             {Name: "def.i", Values: []float64{10}, Type: gostatsd.TIMER, Rate: 1.0, Tags: gostatsd.Tags{"foo"}},
		"smp.rte:5|c|@0.1":                            {Name: "smp.rte", Values: []float64{5}, Type: gostatsd.COUNTER, Rate: 0.1},
		"smp.rte:5|c|@0.1|#foo:bar,baz":               {Name: "smp.rte", Values: []float64{5}, Type: gostatsd.COUNTER, Rate: 0.1, Tags: gostatsd.Tags{"foo:bar", "baz"}},
		"smp.rte:5|c|#foo:bar,baz":                    {Name: "smp.rte", Values: []float64{5}, Type: gostatsd.COUNTER, Rate: 1.0, Tags: gostatsd.Tags{"foo:bar", "baz"}},
		"uniq.usr:joe|s":                              {Name: "uniq.usr", StringValue: "joe", Type: gostatsd.SET, Rate: 1.0},
		"fooBarBaz:2|c":                               {Name: "fooBarBaz", Values: []float64{2}, Type: gostatsd.COUNTER, Rate: 1.0},
		"smp.rte:5|c|#Foo:Bar,baz":                    {Name: "smp.rte", Values: []float64{5}, Type: gostatsd.COUNTER, Rate: 1.0, Tags: gostatsd.Tags{"Foo:Bar", "baz"}},
		"smp.gge:1|g|#Foo:Bar":                        {Name: "smp.gge", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"Foo:Bar"}},
		"smp.gge:1|g|#fo_o:ba-r":                      {Name: "smp.gge", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"fo_o:ba-r"}},
		"smp gge:1|g":                                 {Name: "smp_gge", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"smp/gge:1|g":                                 {Name: "smp-gge", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"smp,gge$:1|g":                                {Name: "smpgge", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"un1qu3:john|s":                               {Name: "un1qu3", StringValue: "john", Type: gostatsd.SET, Rate: 1.0},
		"un1qu3:john|s|#some:42":                      {Name: "un1qu3", StringValue: "john", Type: gostatsd.SET, Rate: 1.0, Tags: gostatsd.Tags{"some:42"}},
		"da-sh:1|s":                                   {Name: "da-sh", StringValue: "1", Type: gostatsd.SET, Rate: 1.0},
		"under_score:1|s":                             {Name: "under_score", StringValue: "1", Type: gostatsd.SET, Rate: 1.0},
		"a:1|g|#f,,":                                  {Name: "a", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"f"}},
		"a:1|g|#,,f":                                  {Name: "a", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"f"}},
		"a:1|g|#f,,z":                                 {Name: "a", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"f", "z"}},
		"a:1|g|#":                                     {Name: "a", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"a:1|g|#,":                                    {Name: "a", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"a:1|g|#,,":                                   {Name: "a", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"foo.bar.baz:2|c|c:xyz":                       {Name: "foo.bar.baz", Values: []float64{2}, Type: gostatsd.COUNTER, Rate: 1.0},
		"smp.rte:5|c|@0.1|c:xyz":                      {Name: "smp.rte", Values: []float64{5}, Type: gostatsd.COUNTER, Rate: 0.1},
		"smp.rte:5|c|@0.1|#foo:bar,baz|c:xyz":         {Name: "smp.rte", Values: []float64{5}, Type: gostatsd.COUNTER, Rate: 0.1, Tags: gostatsd.Tags{"foo:bar", "baz"}},
		"def.i:10|h|#foo|c:xyz":                       {Name: "def.i", Values: []float64{10}, Type: gostatsd.TIMER, Rate: 1.0, Tags: gostatsd.Tags{"foo"}},
		"c.after.tags:1|g|#f,,|c:xyz":                 {Name: "c.after.tags", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"f"}},
		"c.after.tags:1|g|#,,f|c:xyz":                 {Name: "c.after.tags", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"f"}},
		"c.after.tags:1|g|#f,,z|c:xyz":                {Name: "c.after.tags", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0, Tags: gostatsd.Tags{"f", "z"}},
		"c.after.tags:1|g|#|c:xyz":                    {Name: "c.after.tags", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"c.after.tags:1|g|#,|c:xyz":                   {Name: "c.after.tags", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"c.after.tags:1|g|#,,|c:xyz":                  {Name: "c.after.tags", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"c.after.tags:1|g|#,,|c::,#@":                 {Name: "c.after.tags", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"field.order.rev.all:1|g|c:xyz|#foo:bar|@0.1": {Name: "field.order.rev.all", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 0.1, Tags: gostatsd.Tags{"foo:bar"}},
		"field.order.rev.notags:1|g|c:xyz|@0.1":       {Name: "field.order.rev.notags", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 0.1},
		"new.last.prefix:1|g|#,,|c:xyz|x:":            {Name: "new.last.prefix", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"new.last.empty:1|g|#,|c:xyz|":                {Name: "new.last.empty", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"new.last.colon:1|g|#|c:xyz|:":                {Name: "new.last.colon", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"new.first.prefix:1|g|x:#,,|c:xyz|":           {Name: "new.first.prefix", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"new.first.empty:1|g||#,|c:xyz":               {Name: "new.first.empty", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"new.first.colon:1|g|:|#|c:xyz":               {Name: "new.first.colon", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"new.mid.prefix:1|g|#,,|x:|c:xyz":             {Name: "new.mid.prefix", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"new.mid.empty:1|g|#,||c:xyz":                 {Name: "new.mid.empty", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		"new.mid.colon:1|g|#|:|c:xyz":                 {Name: "new.mid.colon", Values: []float64{1}, Type: gostatsd.GAUGE, Rate: 1.0},
		// Value packing tests
		"a.packing:1:2|ms|#|:|c:xyz":   {Name: "a.packing", Values: []float64{1, 2}, Type: gostatsd.TIMER, Rate: 1.0},
		"a.packing:1:2:3|ms|#|:|c:xyz": {Name: "a.packing", Values: []float64{1, 2, 3}, Type: gostatsd.TIMER, Rate: 1.0},
		"a.packing:1:2:|ms|#|:|c:xyz":  {Name: "a.packing", Values: []float64{1, 2}, Type: gostatsd.TIMER, Rate: 1.0},
		"a.packing:|ms|#|:|c:xyz":      {Name: "a.packing", Values: []float64{}, Type: gostatsd.TIMER, Rate: 1.0},
		"a.packing:1:2|c|#|:|c:xyz":    {Name: "a.packing", Values: []float64{1, 2}, Type: gostatsd.COUNTER, Rate: 1.0},
		"a.packing:1:2:3|c|#|:|c:xyz":  {Name: "a.packing", Values: []float64{1, 2, 3}, Type: gostatsd.COUNTER, Rate: 1.0},
		"a.packing:1:2:|c|#|:|c:xyz":   {Name: "a.packing", Values: []float64{1, 2}, Type: gostatsd.COUNTER, Rate: 1.0},
		"a.packing:1:2|g|#|:|c:xyz":    {Name: "a.packing", Values: []float64{1, 2}, Type: gostatsd.GAUGE, Rate: 1.0},
		"a.packing:1:2:3|g|#|:|c:xyz":  {Name: "a.packing", Values: []float64{1, 2, 3}, Type: gostatsd.GAUGE, Rate: 1.0},
		"a.packing:1:2:|g|#|:|c:xyz":   {Name: "a.packing", Values: []float64{1, 2}, Type: gostatsd.GAUGE, Rate: 1.0},
		"a.packing:::|g|#|:|c:xyz":     {Name: "a.packing", Values: []float64{}, Type: gostatsd.GAUGE, Rate: 1.0},
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
		"foo.bar.baz:2|c": {Name: "stats.foo.bar.baz", Values: []float64{2}, Type: gostatsd.COUNTER, Rate: 1.0},
		"abc.def.g:3|g":   {Name: "stats.abc.def.g", Values: []float64{3}, Type: gostatsd.GAUGE, Rate: 1.0},
		"def.g:10|ms":     {Name: "stats.def.g", Values: []float64{10}, Type: gostatsd.TIMER, Rate: 1.0},
		"uniq.usr:joe|s":  {Name: "stats.uniq.usr", StringValue: "joe", Type: gostatsd.SET, Rate: 1.0},
	}

	compareMetric(t, tests, "stats")
}

func TestEventsLexer(t *testing.T) {
	t.Parallel()
	//_e{title.length,text.length}:title|text|d:date_happened|h:hostname|p:priority|t:alert_type|#tag1,tag2
	tests := map[string]gostatsd.Event{
		"_e{1,1}:a|b":                                                           {Title: "a", Text: "b"},
		"_e{6,18}:ab|| c|hello,\\nmy friend!":                                   {Title: "ab|| c", Text: "hello,\nmy friend!"},
		"_e{1,1}:a|b|d:123123":                                                  {Title: "a", Text: "b", DateHappened: 123123},
		"_e{1,1}:a|b|d:123123|h:hoost":                                          {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost"},
		"_e{1,1}:a|b|d:123123|h:hoost|p:low":                                    {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow},
		"_e{1,1}:a|b|d:123123|h:hoost|p:low|t:warning":                          {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning},
		"_e{1,1}:a|b|d:123123|h:hoost|p:low|t:warning|c:xyz":                    {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning},
		"_e{1,1}:a|b|d:123123|h:hoost|p:low|t:warning|#tag1,t:tag2":             {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|d:123123|h:hoost|p:low|t:warning|#tag1,t:tag2|c:xyz":       {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|t:warning|d:123123|h:hoost|p:low|c:xyz|#tag1,t:tag2":       {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|#tag1,t:tag2|t:warning|d:123123|h:hoost|p:low|c:xyz":       {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|c:xyz|#tag1,t:tag2|t:warning|d:123123|h:hoost|p:low":       {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|p:low|c:xyz|#tag1,t:tag2|t:warning|d:123123|h:hoost":       {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|h:hoost|p:low|c:xyz|#tag1,t:tag2|t:warning|d:123123":       {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|t:warning|d:123123|h:hoost|p:low|c:xyz|#tag1,t:tag2|x:unk": {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|x:unk|#tag1,t:tag2|t:warning|d:123123|h:hoost|p:low|c:xyz": {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|c:xyz|x:unk|#tag1,t:tag2|t:warning|d:123123|h:hoost|p:low": {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|p:low|c:xyz|x:unk|#tag1,t:tag2|t:warning|d:123123|h:hoost": {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|h:hoost|p:low|c:xyz|x:unk|#tag1,t:tag2|t:warning|d:123123": {Title: "a", Text: "b", DateHappened: 123123, Source: "hoost", Priority: gostatsd.PriLow, AlertType: gostatsd.AlertWarning, Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|k:agg_key":                                                 {Title: "a", Text: "b", AggregationKey: "agg_key"},
		"_e{1,1}:a|b|h:hoost":                                                   {Title: "a", Text: "b", Source: "hoost"},
		"_e{1,1}:a|b|h:hoost|x:unk":                                             {Title: "a", Text: "b", Source: "hoost"},
		"_e{1,1}:a|b|p:low":                                                     {Title: "a", Text: "b", Priority: gostatsd.PriLow},
		"_e{1,1}:a|b|p:low|x:unk":                                               {Title: "a", Text: "b", Priority: gostatsd.PriLow},
		"_e{1,1}:a|b|t:warning":                                                 {Title: "a", Text: "b", AlertType: gostatsd.AlertWarning},
		"_e{1,1}:a|b|t:warning|x:unk":                                           {Title: "a", Text: "b", AlertType: gostatsd.AlertWarning},
		"_e{1,1}:a|b|#tag1,t:tag2":                                              {Title: "a", Text: "b", Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|#tag1,t:tag2|x:unk":                                        {Title: "a", Text: "b", Tags: []string{"tag1", "t:tag2"}},
		"_e{1,1}:a|b|c:xyz":                                                     {Title: "a", Text: "b"},
		"_e{1,1}:a|b|c:xyz|x:unk":                                               {Title: "a", Text: "b"},
		"_e{1,1}:a|b|x:unk":                                                     {Title: "a", Text: "b"},
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

func TestSeekDelimited(t *testing.T) {
	var stop byte = '|'
	var delimiter byte = ','
	var otherChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ;/\\\".:()-_=?![]'~@$%^&*#"
	var testCases = []struct {
		name     string
		input    []byte
		output   [][]byte
		nextByte byte
	}{
		{"all chars", []byte(otherChars), results([]byte(otherChars)), eof},
		{"all chars + stop", []byte(fmt.Sprintf("%s|", otherChars)), results([]byte(otherChars)), stop},
		{"empty", []byte(""), results([]byte("")), eof},
		{"stop", []byte("|"), results([]byte("")), stop},
		{"multiple stop", []byte("||"), results([]byte("")), stop},
		{"stop + char", []byte("|x"), results([]byte("")), stop},
		{"stop + char |", []byte("|x"), results([]byte("")), stop},
		{"stop + chars", []byte("|foo.bar"), results([]byte("")), stop},
		{"stop + chars + stop", []byte("|foo.bar|"), results([]byte("")), stop},
		{"char", []byte("x"), results([]byte("x")), eof},
		{"char + stop", []byte("x|"), results([]byte("x")), stop},
		{"char + stop + char", []byte("x|y"), results([]byte("x")), stop},
		{"chars", []byte("foo.bar"), results([]byte("foo.bar")), eof},
		{"chars + stop", []byte("foo.bar|"), results([]byte("foo.bar")), stop},
		{"chars + stop + chars", []byte("foo:bar|baz:quux"), results([]byte("foo:bar")), stop},
		{"delimited", []byte("x,foo:bar,baz:quux"), results([]byte("x"), []byte("foo:bar"), []byte("baz:quux")), eof},
		{"delimited + stop", []byte("x,foo:bar,baz:quux|y"), results([]byte("x"), []byte("foo:bar"), []byte("baz:quux")), stop},
		{"empty delimiter", []byte(",foo:bar,,baz:quux,"), results([]byte(""), []byte("foo:bar"), []byte(""), []byte("baz:quux"), []byte("")), eof},
		{"empty delimiter + stop", []byte(",foo:bar,,baz:quux,|y"), results([]byte(""), []byte("foo:bar"), []byte(""), []byte("baz:quux"), []byte("")), stop},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			l := newTestLexer(test.input, "")
			result := make([][]byte, 0)
			for {
				data, complete := seekDelimited(l, stop, delimiter)
				result = append(result, data)
				if complete {
					break
				}
			}

			assert.Equal(t, test.output, result)
			assert.Equal(t, test.nextByte, l.next())
		})
	}
}

func results(results ...[]byte) [][]byte {
	return results
}

func TestSeekUntil(t *testing.T) {
	var stop byte = '|'
	var otherChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ;:/\\\".,()-_=?![]'~@$%^&*#"
	var testCases = []struct {
		name     string
		input    []byte
		output   []byte
		nextByte byte
	}{
		{"all chars", []byte(otherChars), []byte(otherChars), eof},
		{"all chars + stop", []byte(fmt.Sprintf("%s|", otherChars)), []byte(otherChars), stop},
		{"empty", []byte(""), []byte(""), eof},
		{"stop", []byte("|"), []byte(""), stop},
		{"multiple stop", []byte("||"), []byte(""), stop},
		{"stop + char", []byte("|x"), []byte(""), stop},
		{"stop + char |", []byte("|x"), []byte(""), stop},
		{"stop + chars", []byte("|foo.bar"), []byte(""), stop},
		{"stop + chars + stop", []byte("|foo.bar|"), []byte(""), stop},
		{"char", []byte("x"), []byte("x"), eof},
		{"char + stop", []byte("x|"), []byte("x"), stop},
		{"char + stop + char", []byte("x|y"), []byte("x"), stop},
		{"chars", []byte("foo.bar"), []byte("foo.bar"), eof},
		{"chars + stop", []byte("foo.bar|"), []byte("foo.bar"), stop},
		{"chars + stop + chars", []byte("foo.bar|baz:quux"), []byte("foo.bar"), stop},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			l := newTestLexer(test.input, "")
			result := seekUntil(l, stop)
			assert.Equal(t, test.output, result)
			assert.Equal(t, test.nextByte, l.next())
		})
	}
}

func newTestLexer(input []byte, namespace string) *Lexer {
	l := Lexer{
		MetricPool: pool.NewMetricPool(0),
	}
	l.reset()
	l.input = input
	l.namespace = namespace
	l.len = uint32(len(l.input))
	l.sampling = float64(1)

	return &l
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
func BenchmarkParseCounterWithTagsAndContainer(b *testing.B) {
	benchmarkLexer("", "smp.rte:5|c|#foo:bar,baz|c:id", b)
}
func BenchmarkParseCounterWithTagsAndSampleRateAndContainer(b *testing.B) {
	benchmarkLexer("", "smp.rte:5|c|@0.1|#foo:bar,baz|c:id", b)
}

var parselineEventBlackhole *gostatsd.Event

func benchmarkEventLexer(ns string, input string, b *testing.B) {
	slice := []byte(input)
	var e *gostatsd.Event

	l := &Lexer{
		MetricPool: pool.NewMetricPool(0),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, e, _ = l.Run(slice, ns)
	}
	parselineEventBlackhole = e
}

func BenchmarkParseEvent(b *testing.B) {
	benchmarkEventLexer("", "_e{1,1}:a|b|d:123123|h:hoost|p:low|t:warning|#tag1,t:tag2", b)
}
