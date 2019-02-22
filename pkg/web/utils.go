package web

import (
	"bytes"
	"compress/zlib"
)

func decompress(input []byte) ([]byte, error) {
	decompressor, err := zlib.NewReader(bytes.NewReader(input))
	if err != nil {
		return nil, err
	}
	defer decompressor.Close()

	var out bytes.Buffer
	if _, err = out.ReadFrom(decompressor); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
