package statsd

import (
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/jtblin/gostatsd/types"
)

func TestParseLine(t *testing.T) {
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
		"smp gge:1|g":                   {Name: "smp_gge", Value: 1, Type: types.GAUGE},
		"smp/gge:1|g":                   {Name: "smp-gge", Value: 1, Type: types.GAUGE},
		"smp,gge$:1|g":                  {Name: "smpgge", Value: 1, Type: types.GAUGE},
		"un1qu3:john|s":                 {Name: "un1qu3", StringValue: "john", Type: types.SET},
		"un1qu3:john|s|#some:42":        {Name: "un1qu3", StringValue: "john", Type: types.SET, Tags: types.Tags{"some:42"}},
	}

	mr := &MetricReceiver{}
	compare(tests, mr, t)

	failing := []string{"fOO|bar:bazkk", "foo.bar.baz:1|q", "NaN.should.be:NaN|g"}
	for _, tc := range failing {
		result, err := mr.parseLine([]byte(tc))
		if err == nil {
			t.Errorf("test %s: expected error but got %s", tc, result)
		}
	}

	tests = map[string]types.Metric{
		"foo.bar.baz:2|c": {Name: "stats.foo.bar.baz", Value: 2, Type: types.COUNTER},
		"abc.def.g:3|g":   {Name: "stats.abc.def.g", Value: 3, Type: types.GAUGE},
		"def.g:10|ms":     {Name: "stats.def.g", Value: 10, Type: types.TIMER},
		"uniq.usr:joe|s":  {Name: "stats.uniq.usr", StringValue: "joe", Type: types.SET},
	}

	mr = &MetricReceiver{Namespace: "stats"}
	compare(tests, mr, t)

	tests = map[string]types.Metric{
		"foo.bar.baz:2|c":         {Name: "foo.bar.baz", Value: 2, Type: types.COUNTER, Tags: types.Tags{"env:foo"}},
		"abc.def.g:3|g":           {Name: "abc.def.g", Value: 3, Type: types.GAUGE, Tags: types.Tags{"env:foo"}},
		"def.g:10|ms":             {Name: "def.g", Value: 10, Type: types.TIMER, Tags: types.Tags{"env:foo"}},
		"uniq.usr:joe|s":          {Name: "uniq.usr", StringValue: "joe", Type: types.SET, Tags: types.Tags{"env:foo"}},
		"uniq.usr:joe|s|#foo:bar": {Name: "uniq.usr", StringValue: "joe", Type: types.SET, Tags: types.Tags{"env:foo", "foo:bar"}},
	}

	mr = &MetricReceiver{Tags: []string{"env:foo"}}
	compare(tests, mr, t)
}

func compare(tests map[string]types.Metric, mr *MetricReceiver, t *testing.T) {
	for input, expected := range tests {
		result, err := mr.parseLine([]byte(input))
		if err != nil {
			t.Errorf("test %s error: %s", input, err)
			continue
		}
		if !reflect.DeepEqual(result, &expected) {
			t.Errorf("test %s: expected %s, got %s", input, expected, result)
			continue
		}
	}
}

func benchmarkParseLine(mr *MetricReceiver, input string, b *testing.B) {
	for n := 0; n < b.N; n++ {
		mr.parseLine([]byte(input))
	}
}

func BenchmarkParseLineCounter(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{}, "foo.bar.baz:2|c", b)
}
func BenchmarkParseLineCounterWithSampleRate(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{}, "smp.rte:5|c|@0.1", b)
}
func BenchmarkParseLineCounterWithTags(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{}, "smp.rte:5|c|#foo:bar,baz", b)
}
func BenchmarkParseLineCounterWithTagsAndSampleRate(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{}, "smp.rte:5|c|@0.1|#foo:bar,baz", b)
}
func BenchmarkParseLineGauge(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{}, "abc.def.g:3|g", b)
}
func BenchmarkParseLineTimer(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{}, "def.g:10|ms", b)
}
func BenchmarkParseLineSet(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{}, "uniq.usr:joe|s", b)
}
func BenchmarkParseLineCounterWithDefaultTags(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{Tags: []string{"env:foo", "foo:bar"}}, "foo.bar.baz:2|c", b)
}
func BenchmarkParseLineCounterWithDefaultTagsAndTags(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{Tags: []string{"env:foo", "foo:bar"}}, "foo.bar.baz:2|c|#foo:bar,baz", b)
}
func BenchmarkParseLineCounterWithDefaultTagsAndTagsAndNameSpace(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{Namespace: "stats", Tags: []string{"env:foo", "foo:bar"}}, "foo.bar.baz:2|c|#foo:bar,baz", b)
}

type FakeAddr struct{}

func (fa FakeAddr) Network() string { return "udp" }
func (fa FakeAddr) String() string  { return ":8181" }

type FakePacketConn struct{}

func (fpc FakePacketConn) ReadFrom(b []byte) (n int, addr net.Addr, err error) {
	b = []byte("foo.bar.baz:2|c")
	return len(b), FakeAddr{}, nil
}
func (fpc FakePacketConn) WriteTo(b []byte, addr net.Addr) (n int, err error) { return }
func (fpc FakePacketConn) Close() error                                       { return nil }
func (fpc FakePacketConn) LocalAddr() net.Addr                                { return FakeAddr{} }
func (fpc FakePacketConn) SetDeadline(t time.Time) error                      { return nil }
func (fpc FakePacketConn) SetReadDeadline(t time.Time) error                  { return nil }
func (fpc FakePacketConn) SetWriteDeadline(t time.Time) error                 { return nil }

func manageQueue(mq messageQueue) {
	for range mq {
	}
}

// Need to change MetricReceiver.receive to a finite loop to be able to run the benchmark
//func BenchmarkReceive(b *testing.B) {
//	mq := make(messageQueue, maxQueueSize)
//	go manageQueue(mq)
//	mr := &MetricReceiver{}
//	c := FakePacketConn{}
//	b.ResetTimer()
//
//	for n := 0; n < b.N; n++ {
//		mr.receive(c, mq)
//	}
//}
