package transport

import (
	"context"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
)

// TransportOptions allows for the caller to override default behaviors.
type TransportOptions struct {
	// DisableCompression will force compression to be off
	DisableCompression bool

	// Headers allows a caller to pass additional headers
	Headers map[string]string
}

// PostMessage will post a protobuf or json message to the provided URL.
func (hc *Client) PostMessage(ctx context.Context, url string, message interface{}, options *TransportOptions) {
	var headers map[string]string
	disableCompression := hc.debugBody // debug is always uncompressed
	if options != nil {
		headers = options.Headers
		if options.DisableCompression {
			disableCompression = true
		}
	}

	if pb, ok := message.(proto.Message); ok {
		if hc.debugBody {
			hc.postProtobufText(ctx, url, headers, pb)
		} else {
			hc.postProtobuf(ctx, url, headers, disableCompression, pb)
		}
		return
	}
	hc.postJson(ctx, url, headers, disableCompression, message)
}

// postProtobuf will serialize a proto.Message and post it.
func (hc *Client) postProtobuf(ctx context.Context, url string, headers map[string]string, disableCompression bool, message proto.Message) {
	body, err := proto.Marshal(message)
	if err != nil {
		atomic.AddUint64(&hc.messagesFailMarshal.cur, 1)
		return
	}

	encoding := "identity"
	if hc.compress && !disableCompression {
		body, err = compress(body)
		encoding = "deflate"
		if err != nil {
			atomic.AddUint64(&hc.messagesFailCompress.cur, 1)
			return
		}
	}

	hc.PostRaw(ctx, url, "application/x-protobuf", encoding, nil, body)
}

// postProtobufText will serialize a proto.Message in text form and post it.  This is intended for debugging purposes
// only, and the remote side will likely reject the message.
func (hc *Client) postProtobufText(ctx context.Context, url string, headers map[string]string, message proto.Message) {
	body := proto.MarshalTextString(message)
	if body == "" {
		atomic.AddUint64(&hc.messagesFailMarshal.cur, 1)
		return
	}
	contentType := "text/x-protobuf" // FIXME: I'm not sure what the text version of protobuf is meant to be.
	encoding := "identity"           // No compression for the debug rendering
	hc.PostRaw(ctx, url, contentType, encoding, nil, []byte(body))
}

func (hc *Client) postJson(ctx context.Context, url string, headers map[string]string, disableCompression bool, data interface{}) {
	body, err := marshalJson(data, hc.debugBody)
	if err != nil {
		atomic.AddUint64(&hc.messagesFailMarshal.cur, 1)
		return
	}

	encoding := "identity"
	if hc.compress && !disableCompression {
		body, err = compress(body)
		encoding = "deflate"
		if err != nil {
			atomic.AddUint64(&hc.messagesFailCompress.cur, 1)
			return
		}
	}

	hc.PostRaw(ctx, url, "application/json", encoding, nil, body)
}
