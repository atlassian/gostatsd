package statsd

import (
	"context"
	"testing"

	"github.com/atlassian/gostatsd"
	"github.com/stretchr/testify/assert"
)

type TagCapturingHandler struct {
	m []*gostatsd.Metric
	e []*gostatsd.Event
}

func (tch *TagCapturingHandler) EstimatedTags() int {
	return 0
}

func (tch *TagCapturingHandler) DispatchMetric(ctx context.Context, m *gostatsd.Metric) error {
	tch.m = append(tch.m, m)
	return nil
}

func (tch *TagCapturingHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) error {
	tch.e = append(tch.e, e)
	return nil
}

func (tch *TagCapturingHandler) WaitForEvents() {
}

func TestTagMetricHandlerAddsNoTags(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{})
	m := &gostatsd.Metric{}
	th.DispatchMetric(context.Background(), m)
	assert.Equal(t, 1, len(tch.m))         // Metric tracked
	assert.Equal(t, 0, len(tch.m[0].Tags)) // No tags added
	assert.Equal(t, "", tch.m[0].Hostname) // No hostname added
}

func TestTagMetricHandlerAddsSingleTag(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{"tag1"})
	m := &gostatsd.Metric{}
	th.DispatchMetric(context.Background(), m)
	assert.Equal(t, 1, len(tch.m))            // Metric tracked
	assert.Equal(t, 1, len(tch.m[0].Tags))    // 1 tag added
	assert.Equal(t, "tag1", tch.m[0].Tags[0]) //  "tag1" added
	assert.Equal(t, "", tch.m[0].Hostname)    // No hostname added
}

func TestTagMetricHandlerAddsMultipleTags(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{"tag1", "tag2"})
	m := &gostatsd.Metric{}
	th.DispatchMetric(context.Background(), m)
	assert.Equal(t, 1, len(tch.m))            // Metric tracked
	assert.Equal(t, 2, len(tch.m[0].Tags))    // 2 tag added
	assert.Equal(t, "tag1", tch.m[0].Tags[0]) //  "tag1" added
	assert.Equal(t, "tag2", tch.m[0].Tags[1]) //  "tag2" added
	assert.Equal(t, "", tch.m[0].Hostname)    // No hostname added
}

func TestTagMetricHandlerAddsHostname(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{})
	m := &gostatsd.Metric{
		SourceIP: "1.2.3.4",
	}
	th.DispatchMetric(context.Background(), m)
	assert.Equal(t, 1, len(tch.m))                // Metric tracked
	assert.Equal(t, 0, len(tch.m[0].Tags))        // No tags added
	assert.Equal(t, "1.2.3.4", tch.m[0].Hostname) // Hostname injected
}

func TestTagMetricHandlerAddsDuplicateTags(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{"tag1", "tag2", "tag2", "tag3", "tag1"})
	m := &gostatsd.Metric{}
	th.DispatchMetric(context.Background(), m)
	assert.Equal(t, 1, len(tch.m))            // Metric tracked
	assert.Equal(t, 3, len(tch.m[0].Tags))    // 3 tags added
	assert.Equal(t, "tag1", tch.m[0].Tags[0]) //  "tag1" added
	assert.Equal(t, "tag2", tch.m[0].Tags[1]) //  "tag2" added
	assert.Equal(t, "tag3", tch.m[0].Tags[2]) //  "tag3" added
	assert.Equal(t, "", tch.m[0].Hostname)    // No hostname added
}

func TestTagEventHandlerAddsNoTags(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{})
	e := &gostatsd.Event{}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e))         // Metric tracked
	assert.Equal(t, 0, len(tch.e[0].Tags)) // No tags added
	assert.Equal(t, "", tch.e[0].Hostname) // No hostname added
}

func TestTagEventHandlerAddsSingleTag(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{"tag1"})
	e := &gostatsd.Event{}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e))            // Metric tracked
	assert.Equal(t, 1, len(tch.e[0].Tags))    // 1 tag added
	assert.Equal(t, "tag1", tch.e[0].Tags[0]) //  "tag1" added
	assert.Equal(t, "", tch.e[0].Hostname)    // No hostname added
}

func TestTagEventHandlerAddsMultipleTags(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{"tag1", "tag2"})
	e := &gostatsd.Event{}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e))            // Metric tracked
	assert.Equal(t, 2, len(tch.e[0].Tags))    // 2 tag added
	assert.Equal(t, "tag1", tch.e[0].Tags[0]) //  "tag1" added
	assert.Equal(t, "tag2", tch.e[0].Tags[1]) //  "tag2" added
	assert.Equal(t, "", tch.e[0].Hostname)    // No hostname added
}

func TestTagEventHandlerAddsHostname(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{})
	e := &gostatsd.Event{
		SourceIP: "1.2.3.4",
	}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e))                // Metric tracked
	assert.Equal(t, 0, len(tch.e[0].Tags))        // No tags added
	assert.Equal(t, "1.2.3.4", tch.e[0].Hostname) // Hostname injected
}

func TestTagEventHandlerAddsDuplicateTags(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{"tag1", "tag2", "tag2", "tag3", "tag1"})
	e := &gostatsd.Event{}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e))            // Metric tracked
	assert.Equal(t, 3, len(tch.e[0].Tags))    // 3 tags added
	assert.Equal(t, "tag1", tch.e[0].Tags[0]) //  "tag1" added
	assert.Equal(t, "tag2", tch.e[0].Tags[1]) //  "tag2" added
	assert.Equal(t, "tag3", tch.e[0].Tags[2]) //  "tag3" added
	assert.Equal(t, "", tch.e[0].Hostname)    // No hostname added
}

func BenchmarkTagMetricHandlerAddsDuplicateTagsSmall(b *testing.B) {
	tch := &TagCapturingHandler{}
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		th := NewTagHandler(tch, tch, gostatsd.Tags{
			"aaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaa",
			"aaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaa",
			"bbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbb",
		})
		m := &gostatsd.Metric{}
		th.DispatchMetric(context.Background(), m)
	}
}

func BenchmarkTagMetricHandlerAddsDuplicateTagsLarge(b *testing.B) {
	tch := &TagCapturingHandler{}
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		th := NewTagHandler(tch, tch, gostatsd.Tags{
			"aaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaa",
			"cccccccccccccccc:cccccccccccccccc",
			"aaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaa",
			"bbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbb",
			"bbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbb",
			"aaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaa",
			"cccccccccccccccc:cccccccccccccccc",
			"dddddddddddddddd:dddddddddddddddd",
			"dddddddddddddddd:dddddddddddddddd",
			"eeeeeeeeeeeeeeee:eeeeeeeeeeeeeeee",
		})
		m := &gostatsd.Metric{}
		th.DispatchMetric(context.Background(), m)
	}
}

func BenchmarkTagEventHandlerAddsDuplicateTagsSmall(b *testing.B) {
	tch := &TagCapturingHandler{}
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		th := NewTagHandler(tch, tch, gostatsd.Tags{
			"aaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaa",
			"aaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaa",
			"bbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbb",
		})
		e := &gostatsd.Event{}
		th.DispatchEvent(context.Background(), e)
	}
}

func BenchmarkTagEventHandlerAddsDuplicateTagsLarge(b *testing.B) {
	tch := &TagCapturingHandler{}
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		th := NewTagHandler(tch, tch, gostatsd.Tags{
			"aaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaa",
			"cccccccccccccccc:cccccccccccccccc",
			"aaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaa",
			"bbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbb",
			"bbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbb",
			"aaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaa",
			"cccccccccccccccc:cccccccccccccccc",
			"dddddddddddddddd:dddddddddddddddd",
			"dddddddddddddddd:dddddddddddddddd",
			"eeeeeeeeeeeeeeee:eeeeeeeeeeeeeeee",
		})
		e := &gostatsd.Event{}
		th.DispatchEvent(context.Background(), e)
	}
}
