package web

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"

	"github.com/pierrec/lz4/v4"
)

type CompressionType string

const (
	None CompressionType = "None"
	Zlib                 = "Zlib"
	Lz4                  = "Lz4"
)

const (
	ZlibContentEncoding = "deflate"
	Lz4ContentEncoding  = "lz4"
)

func ReadCompressionType(configCompressionType string) (CompressionType, error) {
	switch configCompressionType {
	case "none":
		return None, nil
	case "lz4":
		return Lz4, nil
	case "", "zlib":
		return Zlib, nil
	default:
		// Default to Zlib to maintain existing behaviour
		return Zlib, errors.New("compression type must be one of 'zlib', 'lz4' (default zlib)")
	}
}

func IsValidCompressionLevel(level int) bool {
	// Both lz4 and zlib support levels 0 to 9 (0=Fastest, 9=Best Compression)
	return level >= 0 && level <= 9
}

func ConvertToLZ4CompressionLevel(level int) lz4.CompressionLevel {
	switch level {
	case 0:
		return lz4.Fast
	case 1:
		return lz4.Level1
	case 2:
		return lz4.Level2
	case 3:
		return lz4.Level3
	case 4:
		return lz4.Level4
	case 5:
		return lz4.Level5
	case 6:
		return lz4.Level6
	case 7:
		return lz4.Level7
	case 8:
		return lz4.Level8
	case 9:
		return lz4.Level9
	default:
		return lz4.Fast
	}
}

func CompressWithLz4(in []byte, out io.Writer, compressionLevel int) error {
	compressor := lz4.NewWriter(out)
	err := compressor.Apply(lz4.CompressionLevelOption(ConvertToLZ4CompressionLevel(compressionLevel)))
	if err != nil {
		return err
	}

	if _, err = compressor.Write(in); err != nil {
		_ = compressor.Close()
		return err
	}

	if err = compressor.Close(); err != nil {
		return err
	}
	return nil
}

func CompressWithZlib(in []byte, out io.Writer, compressionLevel int) error {
	compressor, err := zlib.NewWriterLevel(out, compressionLevel)
	if err != nil {
		return err
	}
	_, _ = compressor.Write(in) // error is propagated through Close
	err = compressor.Close()
	if err != nil {
		return err
	}
	return nil
}

func DecompressWithZlib(input []byte) ([]byte, error) {
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

func DecompressWithLz4(input []byte) ([]byte, error) {
	decompressor := lz4.NewReader(bytes.NewReader(input))
	var out bytes.Buffer
	if _, err := out.ReadFrom(decompressor); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
