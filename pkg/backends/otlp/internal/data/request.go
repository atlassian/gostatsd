package data

import (
	"bytes"
	"context"
	"net/http"
)

func withHeader(key, value string) func(*http.Request) {
	return func(req *http.Request) {
		req.Header.Set(key, value)
	}
}

func createProtobufRequest(ctx context.Context, endpoint string, buf []byte, option ...func(*http.Request)) (*http.Request, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint,
		bytes.NewBuffer(buf),
	)
	if err != nil {
		return nil, err
	}

	req.Close = true
	req.Header.Set("Content-Type", RequestContentTypeProtobuf)
	for _, opt := range option {
		opt(req)
	}
	return req, nil
}
