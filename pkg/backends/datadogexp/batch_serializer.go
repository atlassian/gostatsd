package datadogexp

import (
	"bytes"
	"compress/zlib"
	"context"

	"github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
)

// renderedBody represents a ddTimeSeries serialized to a []byte, with optional zlib compression applied.
type renderedBody struct {
	body       []byte
	compressed bool
}

type batchSerializer struct {
	logger   logrus.FieldLogger
	compress bool

	receiveBatch <-chan ddTimeSeries
	submitBody   chan<- *renderedBody

	json jsoniter.API
}

func (bs *batchSerializer) Run(ctx context.Context) {
	bs.logger.Info("Starting")
	defer bs.logger.Info("Terminating")

	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-bs.receiveBatch:
			var renderedBody *renderedBody
			var err error
			if bs.compress {
				renderedBody, err = bs.processBatchCompress(batch)
			} else {
				renderedBody, err = bs.processBatch(batch)
			}
			if err == nil {
				select {
				case <-ctx.Done():
					return
				case bs.submitBody <- renderedBody:
				}
			} else {
				bs.logger.WithFields(logrus.Fields{
					"error": err,
					"size":  len(batch.Series),
				}).Error("failed to serialize batch")
			}
		}
	}
}

func (bs *batchSerializer) processBatch(batch ddTimeSeries) (*renderedBody, error) {
	buf := &bytes.Buffer{}
	jsonWriter := bs.json.BorrowStream(buf)
	jsonWriter.WriteVal(batch)
	err := jsonWriter.Flush()
	bs.json.ReturnStream(jsonWriter)
	if err != nil {
		return nil, err
	}
	return &renderedBody{
		body:       buf.Bytes(),
		compressed: false,
	}, nil
}

func (bs *batchSerializer) processBatchCompress(batch ddTimeSeries) (*renderedBody, error) {
	buf := &bytes.Buffer{}
	compressor, err := zlib.NewWriterLevel(buf, zlib.BestCompression)
	if err != nil {
		return nil, err
	}
	jsonWriter := bs.json.BorrowStream(compressor)
	jsonWriter.WriteVal(batch)
	err = jsonWriter.Flush()
	bs.json.ReturnStream(jsonWriter)
	if err != nil {
		return nil, err
	}
	err = compressor.Close()
	if err != nil {
		return nil, err
	}
	return &renderedBody{
		body:       buf.Bytes(),
		compressed: true,
	}, nil
}
