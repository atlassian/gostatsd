// Package tools is not intended to be consumed by any other package
// but used to help pin tools.

//go:build tools

package tools

import (
	_ "github.com/dvyukov/go-fuzz/go-fuzz"
	_ "github.com/dvyukov/go-fuzz/go-fuzz-build"
	_ "github.com/golangci/golangci-lint/cmd/golangci-lint"
	_ "github.com/jstemmer/go-junit-report"
	_ "github.com/mattn/goveralls"
	_ "golang.org/x/tools/cmd/goimports"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)
