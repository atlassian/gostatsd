package promremotewriter

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pb"
)

// flush represents a send operation.
type flush struct {
	writeRequest    *pb.PromWriteRequest
	metricsPerBatch uint
	timestamp       int64
	cb              func(*pb.PromWriteRequest)
}

// tagsAsLabels will process a Tags in to prom style labels for
// remote write.  The following rules are applied to the keys:
// 1a. tags that aren't key:value ("value") are normalized as "unnamed:value"
// 1b. tags with no key (":value") are normalized as "unnamed:value"
// 1c. tags with no value ("key:") are normalized as "key:__unset__"
// 2. if there is a tag of the form "unnamed:x", it will be combined with the regular unnamed values
// 3. duplicate values for the same key are sorted (including "unnamed" tags)
// 4. keys with multiple values have their values concatenated with __
// 5. the final output is sorted by keys
//
// Example:
// ["foo", "key:bar", "unnamed:baz", "key:thing", "other:something"]
//
// Process unnamed, sort values, and concatenate to a single value:
// unnamed: ["foo", "baz"] -> sort as ["baz", "foo"] -> concatenate as "unnamed=baz__foo"
// key: ["bar", "thing"] -> sort as ["bar", "thing"] -> concatenate as "key=bar__thing"
// other: ["something"] -> sort as ["something"] -> concatenate as "other=something"
//
// Sort by key:
// [{key: bar__thing}, {other: something}, {unnamed: baz__foo}]

func tagsAsLabels(metricName string, tags gostatsd.Tags) []*pb.PromLabel {
	newTags := make(map[string][]string, len(tags)+1)

	for _, tag := range tags {
		kv := strings.SplitN(tag, ":", 2)
		var key, value string
		if len(kv) == 1 {
			key = "unnamed"
			value = kv[0]
		} else if kv[0] == "" {
			key = "unnamed"
			value = kv[1]
		} else {
			key = sanitizeLabelName(kv[0])
			value = kv[1]
		}
		if value == "" {
			value = "__unset__"
		}
		newTags[key] = append(newTags[key], value)
	}

	// If someone has a tag with this name, they get what they deserve.
	newTags["__name__"] = []string{sanitizeMetricName(metricName)}

	keys := make([]string, 0, len(newTags))
	for key, values := range newTags {
		keys = append(keys, key)
		sort.Strings(values)
	}
	sort.Strings(keys)
	labels := make([]*pb.PromLabel, 0, len(keys))
	for _, key := range keys {
		labels = append(labels, &pb.PromLabel{
			Name:  key,
			Value: strings.Join(newTags[key], "__"),
		})
	}
	return labels
}

func sanitizeLabelName(labelName string) string {
	// Label names MUST adhere to the regex `[a-zA-Z_]([a-zA-Z0-9_])*`

	// There's way to optimize the sanitize* functions, but they're not actually
	// called that much - primarily on flush, and only post-aggregation, so it's
	// not worth the cognitive load of a more complicated function.
	var sb strings.Builder
	sb.Grow(len(labelName))
	for idx, ch := range labelName {
		if unicode.IsLetter(ch) || (ch == '_') || (unicode.IsNumber(ch) && idx > 0) {
			sb.WriteRune(ch)
		} else {
			sb.WriteByte('_')
		}
	}
	return sb.String()
}

func sanitizeMetricName(metricName string) string {
	// Metric names MUST adhere to the regex `[a-zA-Z_:]([a-zA-Z0-9_:])*`
	var sb strings.Builder
	sb.Grow(len(metricName))
	for idx, ch := range metricName {
		if unicode.IsLetter(ch) || (ch == '_') || (ch == ':') || (unicode.IsNumber(ch) && idx > 0) {
			sb.WriteRune(ch)
		} else {
			sb.WriteByte('_')
		}
	}
	return sb.String()
}

// addMetricf adds a metric to the series.
func (f *flush) addMetricf(value float64, tags gostatsd.Tags, nameFormat string, a ...interface{}) {
	f.addMetric(value, tags, fmt.Sprintf(nameFormat, a...))
}

// addMetric adds a metric to the series.
// If the value is non-numeric (in the case of NaN and Inf values), the value is coerced into a numeric value.
func (f *flush) addMetric(value float64, tags gostatsd.Tags, name string) {
	f.writeRequest.Timeseries = append(f.writeRequest.Timeseries,
		&pb.PromTimeSeries{
			Labels: tagsAsLabels(name, tags),
			Samples: []*pb.PromSample{{
				Value:     coerceToNumeric(value),
				Timestamp: f.timestamp,
			}},
		},
	)
}

// coerceToNumeric will convert non-numeric NaN and Inf values to a numeric value.
// If v is a numeric, the same value is returned.
func coerceToNumeric(v float64) float64 {
	if math.IsNaN(v) {
		return -1
	} else if math.IsInf(v, 1) {
		return math.MaxFloat64
	} else if math.IsInf(v, -1) {
		return -math.MaxFloat64
	}
	return v
}

func (f *flush) maybeFlush() {
	if uint(len(f.writeRequest.Timeseries))+20 >= f.metricsPerBatch { // flush before it reaches max size and grows the slice
		f.cb(f.writeRequest)
		f.writeRequest = &pb.PromWriteRequest{}
	}
}

func (f *flush) finish() {
	if len(f.writeRequest.Timeseries) > 0 {
		f.cb(f.writeRequest)
	}
}
