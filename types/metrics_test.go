package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexOfKey(t *testing.T) {
	assert := assert.New(t)

	tags := Tags{"foo", "bar:baz", "baz"}
	idx, value := tags.IndexOfKey("bar")

	assert.Equal(1, idx)
	assert.Equal("bar:baz", value)

	tags = Tags{"foo", "bar:baz", "baz"}
	idx, value = tags.IndexOfKey("foobar")

	assert.Equal(-1, idx)
	assert.Equal("", value)
}

func TestExtractSourceFromTags(t *testing.T) {
	assert := assert.New(t)

	stags := "foo,statsd_source_ip:1.2.3.4,baz"
	source, tags := ExtractSourceFromTags(stags)

	assert.Equal("1.2.3.4", source)
	assert.Equal(Tags{"foo", "baz"}, tags)

	stags = "foo,source_ip:1.2.3.4,baz"
	source, tags = ExtractSourceFromTags(stags)

	assert.Equal("", source)
	assert.Equal(Tags{"foo", "source_ip:1.2.3.4", "baz"}, tags)
}
