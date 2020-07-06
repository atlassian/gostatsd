package influxdb

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/atlassian/gostatsd"
)

// flush represents a send operation.
type flush struct {
	buffer           *bytes.Buffer
	writer           io.WriteCloser
	metricCount      uint64
	metricsPerBatch  uint64
	timestampSeconds int64
	flushIntervalSec float64
	disabledSubtypes gostatsd.TimerSubtypes
	errorCounter     *uint64
	cb               func(buf *bytes.Buffer, seriesCount uint64)
	getBuffer        func() (*bytes.Buffer, io.WriteCloser)
	releaseBuffer    func(buf *bytes.Buffer)
}

// formatNameTags will format a measurement name and tags in an appropriate
// form for InfluxDB.  The following rules are applied to the keys:
// - tags with only a value are normalized as "unnamed:value"
// - if there is a tag of the form "unnamed:x", it will be combined with the regular unnamed values
// - duplicate values for the same key are sorted (including "unnamed" tags)
// - keys with multiple values have their values concatenated with __
// - the final output is sorted by keys
//
// Example:
// ["foo", "key:bar", "unnamed:baz", "key:thing", "other:something"]
//
// Process unnamed, sort values, and concatenate to a single value:
// unnamed: ["foo", "baz"] -> sort as ["baz", "foo"] -> concatenate as "unnamed=baz__foo"
// key: ["bar", "thing"] -> sort as ["bar", "thing"] -> concatenate as "key=bar__thing"
// other: ["something"] -> sort as ["something"] -> concatenate as "other=something"
//
// Sort by key and merge:
// key=bar__thing,other=something,unnamed=baz__foo
//
// The name will be prepended, and a space will be appended.
func formatNameTags(name string, tags gostatsd.Tags) string {
	newTags := make(map[string][]string, len(tags))
	for _, tag := range tags {
		kv := strings.SplitN(tag, ":", 2)

		var key, value string
		if len(kv) == 1 {
			key = "unnamed"
			value = kv[0]
		} else {
			key = kv[0]
			value = kv[1]
		}
		newTags[key] = append(newTags[key], value)
	}

	sb := &strings.Builder{}
	escapeNameToBuilder(sb, name)
	keys := make([]string, 0, len(newTags))
	for key, values := range newTags {
		keys = append(keys, key)
		sort.Strings(values)
	}
	sort.Strings(keys)
	for _, key := range keys {
		sb.WriteByte(',')
		escapeTagToBuilder(sb, key)
		sb.WriteByte('=')
		for idx, value := range newTags[key] {
			if idx > 0 {
				sb.WriteString("__")
			}
			escapeTagToBuilder(sb, value)
		}
	}
	sb.WriteByte(' ')
	return sb.String()
}

// escape will escape NL (\n), CR (\r), TAB (\t), space, comma, backslash, and equals
// writing the results to the supplied strings.Builder.
func escapeTagToBuilder(sb *strings.Builder, s string) {
	for _, ch := range s {
		if ch == '\n' {
			sb.WriteString("\\n")
		} else if ch == '\r' {
			sb.WriteString("\\r")
		} else if ch == '\t' {
			sb.WriteString("\\t")
		} else {
			if ch == ' ' || ch == ',' || ch == '\\' || ch == '=' {
				sb.WriteByte('\\')
			}
			sb.WriteRune(ch)
		}
	}
}

// escape will escape NL (\n), CR (\r), TAB (\t), space, comma, and backslash,
// writing the results to the supplied strings.Builder.
func escapeNameToBuilder(sb *strings.Builder, s string) {
	for _, ch := range s {
		if ch == '\n' {
			sb.WriteString("\\n")
		} else if ch == '\r' {
			sb.WriteString("\\r")
		} else if ch == '\t' {
			sb.WriteString("\\t")
		} else {
			if ch == ' ' || ch == ',' || ch == '\\' {
				sb.WriteByte('\\')
			}
			sb.WriteRune(ch)
		}
	}
}

// escapeStringToBuilder will escape double quotes and backslash writing the
// results to the supplied strings.Builder, surrounded by double-quotes
func escapeStringToBuilder(sb *strings.Builder, s string) {
	sb.WriteByte('"')
	for _, ch := range s {
		if ch == '\\' || ch == '"' {
			sb.WriteByte('\\')
		}
		sb.WriteRune(ch)
	}
	sb.WriteByte('"')
}

// writeName writes "measurement,tag1=value1,tag2=value2,... " to the output buffer.
// Common to all metric types and events.
func writeName(w io.Writer, name string, tags gostatsd.Tags) {
	_, _ = w.Write([]byte(formatNameTags(name, tags)))
}

func (f *flush) addCounter(name string, tags gostatsd.Tags, count int64, rate float64) {
	writeName(f.writer, name, tags)
	_, _ = f.writer.Write([]byte(fmt.Sprintf("count=%d,rate=%g %d\n", count, rate, f.timestampSeconds)))
	f.metricCount++
	f.maybeFlush()
}

func (f *flush) addGauge(name string, tags gostatsd.Tags, value float64) {
	writeName(f.writer, name, tags)
	_, _ = f.writer.Write([]byte(fmt.Sprintf("value=%g %d\n", value, f.timestampSeconds)))
	f.metricCount++
	f.maybeFlush()
}

func (f *flush) addSet(name string, tags gostatsd.Tags, value uint64) {
	writeName(f.writer, name, tags)
	_, _ = f.writer.Write([]byte(fmt.Sprintf("count=%d %d\n", value, f.timestampSeconds)))
	f.metricCount++
	f.maybeFlush()
}

func (f *flush) addBaseTimer(name string, timer gostatsd.Timer) {
	var sb strings.Builder
	if !f.disabledSubtypes.Lower {
		sb.WriteString(fmt.Sprintf("lower=%g,", timer.Min))
	}
	if !f.disabledSubtypes.Upper {
		sb.WriteString(fmt.Sprintf("upper=%g,", timer.Max))
	}
	if !f.disabledSubtypes.Count {
		sb.WriteString(fmt.Sprintf("count=%d,", timer.Count))
	}
	if !f.disabledSubtypes.CountPerSecond {
		sb.WriteString(fmt.Sprintf("rate=%g,", timer.PerSecond))
	}
	if !f.disabledSubtypes.Mean {
		sb.WriteString(fmt.Sprintf("mean=%g,", timer.Mean))
	}
	if !f.disabledSubtypes.Median {
		sb.WriteString(fmt.Sprintf("median=%g,", timer.Median))
	}
	if !f.disabledSubtypes.StdDev {
		sb.WriteString(fmt.Sprintf("stddev=%g,", timer.StdDev))
	}
	if !f.disabledSubtypes.Sum {
		sb.WriteString(fmt.Sprintf("sum=%g,", timer.Sum))
	}
	if !f.disabledSubtypes.SumSquares {
		sb.WriteString(fmt.Sprintf("sum_squares=%g,", timer.SumSquares))
	}
	for _, pct := range timer.Percentiles {
		sb.WriteString(fmt.Sprintf("%s=%g,", pct.Str, pct.Float))
	}
	if sb.Len() == 0 { // wat
		return
	}
	writeName(f.writer, name, timer.Tags)
	buf := sb.String()
	_, _ = f.writer.Write([]byte(fmt.Sprintf("%s %d\n", buf[:len(buf)-1], f.timestampSeconds)))
	f.metricCount++
	f.maybeFlush()
}

func (f *flush) addHistogramTimer(name string, timer gostatsd.Timer) {
	writeName(f.writer, name, timer.Tags)

	var sb strings.Builder
	for histogramThreshold, count := range timer.Histogram {
		bucket := "le.+Inf"
		if !math.IsInf(float64(histogramThreshold), 1) {
			bucket = "le." + strconv.FormatFloat(float64(histogramThreshold), 'f', -1, 64)
		}
		sb.WriteString(bucket)
		sb.WriteByte('=')
		sb.WriteString(strconv.Itoa(count))
		sb.WriteByte(',')
	}
	buf := sb.String()
	_, _ = f.writer.Write([]byte(fmt.Sprintf("%s %d\n", buf[:len(buf)-1], f.timestampSeconds)))
	f.metricCount++
	f.maybeFlush()
}

func (f *flush) maybeFlush() {
	if f.metricCount >= f.metricsPerBatch {
		f.flush()
	}
}

func (f *flush) finish() {
	if f.metricCount > 0 {
		f.flush()
	}
	f.releaseBuffer(f.buffer)
}

func (f *flush) flush() {
	// I don't believe that a gzip.Writer sending to a bytes.Buffer can actually fail, but the
	// flate.huffmanBitWriter at the core of the object graph *does* have an internal error it
	// can theoretically produce, so we won't ignore the error.
	err := f.writer.Close()
	if err == nil {
		f.cb(f.buffer, f.metricCount)
	} else {
		atomic.AddUint64(f.errorCounter, 1)
		f.releaseBuffer(f.buffer)
	}
	f.metricCount = 0
	f.buffer, f.writer = f.getBuffer()
}
