BUILD_DATE          := $(shell date +%Y-%m-%d-%H:%M)
REPO_VERSION        ?= $(shell git describe --abbrev=0 --tags)
GIT_HASH            ?= $(shell git rev-parse --short HEAD)
BINARY_NAME         := gostatsd
CPU_ARCH            ?= amd64
CGO_ENABLED         ?= 0
REPOSITORY_NAME     := atlassianlabs/$(BINARY_NAME)
REGISTRY_NAME       := docker-public.packages.atlassian.com
IMAGE_PREFIX        := $(REGISTRY_NAME)/$(REPOSITORY_NAME)
IMAGE_NAME          := $(IMAGE_PREFIX)-$(CPU_ARCH)
ARCH                ?= $(shell uname -s | tr A-Z a-z)
GOVERSION           := 1.24.6  # Go version needs to be the same in: CI config, README, Dockerfiles, and Makefile
GP                  := /gopath
MAIN_PKG            := github.com/atlassian/gostatsd/cmd/gostatsd
CLUSTER_PKG         := github.com/atlassian/gostatsd/cmd/cluster
LAMBDA_PKG          := github.com/atlassian/gostatsd/cmd/lambda-extension
PROTOBUF_VERSION    ?= 26.1
PROJECT_ROOT_DIR    := $(realpath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
TOOLS_DIR           := $(PROJECT_ROOT_DIR)/.tools

.tools/bin/protoc:
	mkdir -p $(TOOLS_DIR)
	curl -L -O https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOBUF_VERSION)/protoc-$(PROTOBUF_VERSION)-linux-x86_64.zip
	unzip -o -d $(TOOLS_DIR) protoc-$(PROTOBUF_VERSION)-linux-x86_64.zip
	rm protoc-$(PROTOBUF_VERSION)-linux-x86_64.zip

pb/gostatsd.pb.go: pb/gostatsd.proto .tools/bin/protoc
	@# protoc requires the protoc-gen-go plugin to be on the PATH.  In order to maintain the
	@# usage of `go tool` and properly pinned versions, we install protoc-gen-go to a temporary
	@# directory, run protoc, then clean up the temporary directory.
	@#
	@# Note: go install will use the version defined in go.mod
	@TMPBIN=$(shell pwd)/$(shell mktemp -d protoc-gen-go.XXXXXXXXXX)                          ; \
	GOBIN=$$TMPBIN go install google.golang.org/protobuf/cmd/protoc-gen-go                    ; \
	PATH=$$TMPBIN:$$PATH $(TOOLS_DIR)/bin/protoc --go_out=. --go_opt=paths=source_relative $< ; \
	rm $$TMPBIN/protoc-gen-go                                                                 ; \
	rmdir $$TMPBIN

build:
	CGO_ENABLED=$(CGO_ENABLED) GOEXPERIMENT=boringcrypto \
		go build  \
			-v \
			-ldflags "-X main.Version=$(REPO_VERSION) -X main.GitCommit=$(GIT_HASH) -X main.BuildDate=$(BUILD_DATE)" \
			$(GOBUILD_OPTIONAL_FLAGS) \
			-o bin/$(ARCH)/$(CPU_ARCH)/$(BINARY_NAME) \
			$(PKG)

build-gostatsd:
	@$(MAKE) build PKG=$(MAIN_PKG)

build-lambda:
	@$(MAKE) build PKG=$(LAMBDA_PKG) BINARY_NAME="lambda-extension"

build-gostatsd-race:
	@$(MAKE) build-gostatsd GOBUILD_OPTIONAL_FLAGS="-race"

build-cluster:
	@$(MAKE) build PKG=$(CLUSTER_PKG) BINARY_NAME="cluster"

build-gostatsd-fips:
	@$(MAKE) build-gostatsd GOBUILD_OPTIONAL_FLAGS="-tags fips"

build-lambda-fips:
	@$(MAKE) build PKG=$(LAMBDA_PKG) BINARY_NAME="lambda-extension" GOBUILD_OPTIONAL_FLAGS="-tags fips"

test-all: check-fmt cover test-race bench bench-race check

test-all-full: check-fmt cover test-race-full bench-full bench-race-full check

check-fmt:
	@# This has three quirks:
	@# 1. gci reads from stdin if it's not a character device (ie, if it's a file).  By redirecting /dev/null, it won't read from it
	@# 2. gofmt and gci don't return 1 for failure, so the `| ( ! grep . )` will return 1 if there is any output at all, failing the step
	@# 3. if there's actual errors being printed, we want the stderr to go to stdout so it triggers #2
	@go mod tidy
	@echo Checking for any changes...
	gofmt -d $$(find . -type f -name '*.go' -not -path "./vendor/*" -not -path "./pb/*")                                    2>&1 | ( ! grep .)
	go tool github.com/daixiang0/gci diff . -s standard -s default -s localmodule --skip-generated --skip-vendor </dev/null 2>&1 | ( ! grep .)

fix-fmt:
	gofmt -w $$(find . -type f -name '*.go' -not -path "./vendor/*" -not -path "./pb/*")
	go tool github.com/daixiang0/gci diff . -s standard -s default -s localmodule --skip-generated --skip-vendor

test-full: pb/gostatsd.pb.go
	go test ./...

test-race-full: pb/gostatsd.pb.go
	go test -race ./...

bench-full: pb/gostatsd.pb.go
	go test -bench=. -run=XXX ./...

bench-race-full: pb/gostatsd.pb.go
	go test -race -bench=. -run=XXX ./...

test: pb/gostatsd.pb.go
	go test -short ./...

test-race: pb/gostatsd.pb.go
	go test -short -race ./...

bench: pb/gostatsd.pb.go
	go test -short -bench=. -run=XXX ./...

bench-race: pb/gostatsd.pb.go
	go test -short -race -bench=. -run=XXX ./...

cover: pb/gostatsd.pb.go
	./cover.sh
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out

junit-test: build
	go test -short -v ./... | go tool github.com/jstemmer/go-junit-report > test-report.xml

check: pb/gostatsd.pb.go
	go install ./cmd/gostatsd
	go install ./cmd/tester

check-all: pb/gostatsd.pb.go
	go install ./cmd/gostatsd
	go install ./cmd/tester

fuzz:
	go tool github.com/dvyukov/go-fuzz/go-fuzz-build github.com/atlassian/gostatsd/internal/lexer
	go tool github.com/dvyukov/go-fuzz/go-fuzz -bin=./lexer-fuzz.zip -workdir=test_fixtures/lexer_fuzz

git-hook:
	cp dev/push-hook.sh .git/hooks/pre-push

build-hash: pb/gostatsd.pb.go
	docker buildx build -t $(IMAGE_NAME):$(GIT_HASH) -f build/Dockerfile-multiarch \
		--build-arg MAIN_PKG=$(MAIN_PKG) \
		--build-arg BINARY_NAME=$(BINARY_NAME) \
		--platform=linux/$(CPU_ARCH) \
		--load .
	docker images

build-hash-race: pb/gostatsd.pb.go
	docker buildx build -t $(IMAGE_NAME):$(GIT_HASH)-race -f build/Dockerfile-multiarch-glibc \
		--build-arg MAIN_PKG=$(MAIN_PKG) \
		--build-arg BINARY_NAME=$(BINARY_NAME) \
		--platform=linux/$(CPU_ARCH) \
		--load .
	docker images

release-hash-ci: build-hash
	docker push $(IMAGE_NAME):$(GIT_HASH)

release-normal-ci: release-hash-ci
	docker tag $(IMAGE_NAME):$(GIT_HASH) $(IMAGE_NAME):latest
	docker push $(IMAGE_NAME):latest
	docker tag $(IMAGE_NAME):$(GIT_HASH) $(IMAGE_NAME):$(REPO_VERSION)
	docker push $(IMAGE_NAME):$(REPO_VERSION)

release-hash-race-ci: build-hash-race
	docker push $(IMAGE_NAME):$(GIT_HASH)-race

release-race-ci: release-hash-race-ci
	docker tag $(IMAGE_NAME):$(GIT_HASH)-race $(IMAGE_NAME):$(REPO_VERSION)-race
	docker push $(IMAGE_NAME):$(REPO_VERSION)-race

# Only works in Github actions, which is the only place `make release-ci` should be run
docker-login-ci:
	echo "$$ARTIFACTORY_API_KEY" | docker login docker-public.packages.atlassian.com \
	  --username ${ARTIFACTORY_USERNAME} \
	  --password-stdin

release-ci: docker-login-ci release-normal-ci release-race-ci

release-manifest-ci: docker-login-ci
	for tag in latest $(REPO_VERSION) $(GIT_HASH)-race $(REPO_VERSION)-race ; do \
		docker pull $(IMAGE_NAME):$$tag ; \
		docker manifest create $(IMAGE_PREFIX):$$tag --amend \
			$(IMAGE_NAME):$$tag ; \
	  	docker manifest push $(IMAGE_PREFIX):$$tag ; \
	done

run: build
	./build/bin/$(ARCH)/$(BINARY_NAME) --backends=stdout --verbose --flush-interval=2s

run-docker: docker
	docker-compose rm -f build/gostatsd
	docker-compose -f build/docker-compose.yml build
	docker-compose -f build/docker-compose.yml up -d

stop-docker:
	cd build/ && docker-compose stop

version:
	@echo $(REPO_VERSION)

clean:
	rm -rf bin/*
	-docker rm $(docker ps -a -f 'status=exited' -q)
	-docker rmi $(docker images -f 'dangling=true' -q)

.PHONY: build
