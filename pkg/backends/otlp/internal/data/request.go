package data

import (
	"bytes"
	"context"
	"net/http"
)

func createProtobufRequest(ctx context.Context, endpoint string, buf []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint,
		bytes.NewBuffer(buf),
	)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", RequestContentTypeProtobuf)
	return req, nil
}
