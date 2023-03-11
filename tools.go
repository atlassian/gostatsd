//go:build tools

package gostatsd

// https://github.com/golang/go/wiki/Modules#how-can-i-track-tool-dependencies-for-a-module

import (
	_ "github.com/dvyukov/go-fuzz/go-fuzz"
	_ "github.com/dvyukov/go-fuzz/go-fuzz-build"
	_ "github.com/githubnemo/CompileDaemon"
	_ "github.com/golangci/golangci-lint/cmd/golangci-lint"
	_ "github.com/jstemmer/go-junit-report"
	_ "golang.org/x/tools/cmd/goimports"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)
