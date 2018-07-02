package datadogexp

import (
	"context"
	"testing"
	"time"

	"github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func batchSerialize(t *testing.T, compress bool, ts ddTimeSeries) *renderedBody {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	sendBatch := make(chan ddTimeSeries)
	recvBatch := make(chan *renderedBody)
	bs := &batchSerializer{
		logger:       logrus.StandardLogger(),
		compress:     compress,
		receiveBatch: sendBatch,
		submitBody:   recvBatch,
		json: jsoniter.Config{
			EscapeHTML:  false,
			SortMapKeys: false,
		}.Froze(),
	}

	go bs.Run(ctx)

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout submitting timeseries")
	case sendBatch <- ts:
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout receiving body")
	case rb := <-recvBatch:
		return rb
	}
	return nil
}

var emptyBlob = []byte(`{"series":null}`)

func TestBatchSerializerCompress(t *testing.T) {
	rb := batchSerialize(t, true, ddTimeSeries{})
	// Exactly what it looks like may vary, so make sure it doesn't match an empty blob
	require.NotEqual(t, emptyBlob, rb.body)
	require.Equal(t, true, rb.compressed)
}

func TestBatchSerializerNoCompress(t *testing.T) {
	rb := batchSerialize(t, false, ddTimeSeries{})
	require.Equal(t, emptyBlob, rb.body)
	require.Equal(t, false, rb.compressed)
}

//func TestBatchSerializer
