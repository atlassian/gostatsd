GO111MODULE := on
VERSION_VAR := main.Version
GIT_VAR := main.GitCommit
BUILD_DATE_VAR := main.BuildDate
BUILD_DATE := $$(date +%Y-%m-%d-%H:%M)
REPO_VERSION ?= $$(git describe --abbrev=0 --tags)
GIT_HASH ?= $$(git rev-parse --short HEAD)
GOBUILD_VERSION_ARGS := -ldflags "-s -X $(VERSION_VAR)=$(REPO_VERSION) -X $(GIT_VAR)=$(GIT_HASH) -X $(BUILD_DATE_VAR)=$(BUILD_DATE)"
GOBUILD_VERSION_ARGS_WITH_SYMS := -ldflags "-X $(VERSION_VAR)=$(REPO_VERSION) -X $(GIT_VAR)=$(GIT_HASH) -X $(BUILD_DATE_VAR)=$(BUILD_DATE)"
BINARY_NAME := gostatsd
IMAGE_NAME := atlassianlabs/$(BINARY_NAME)
ARCH ?= $$(uname -s | tr A-Z a-z)
GOVERSION := 1.13.6  # Keep in sync with .travis.yml
GP := /gopath
MAIN_PKG := github.com/atlassian/gostatsd/cmd/gostatsd
CLUSTER_PKG := github.com/atlassian/gostatsd/cmd/cluster
PROTOBUF_VERSION ?= 3.6.1

setup: setup-ci
	go get github.com/githubnemo/CompileDaemon
	go get github.com/jstemmer/go-junit-report
	go get golang.org/x/tools/cmd/goimports

tools/bin/protoc:
	curl -L -O https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOBUF_VERSION)/protoc-$(PROTOBUF_VERSION)-linux-x86_64.zip
	unzip -d tools/ protoc-$(PROTOBUF_VERSION)-linux-x86_64.zip
	rm protoc-$(PROTOBUF_VERSION)-linux-x86_64.zip

setup-ci: tools/bin/protoc
	# Unpin this when gitea 1.10 is released
	# https://github.com/go-gitea/gitea/issues/8126
	# https://github.com/atlassian/gostatsd/issues/266
	go get github.com/golangci/golangci-lint/cmd/golangci-lint@v1.18.0

build-cluster: fmt
	go build -i -v -o build/bin/$(ARCH)/cluster $(GOBUILD_VERSION_ARGS) $(CLUSTER_PKG)

pb/gostatsd.pb.go: pb/gostatsd.proto
	go build -o protoc-gen-go github.com/golang/protobuf/protoc-gen-go/ && \
	    tools/bin/protoc --go_out=. $< && \
	    rm protoc-gen-go

build: pb/gostatsd.pb.go fmt
	go build -i -v -o build/bin/$(ARCH)/$(BINARY_NAME) $(GOBUILD_VERSION_ARGS) $(MAIN_PKG)

build-race: fmt
	go build -i -v -race -o build/bin/$(ARCH)/$(BINARY_NAME) $(GOBUILD_VERSION_ARGS) $(MAIN_PKG)

build-all: pb/gostatsd.pb.go
	go install -v ./...

test-all: fmt test test-race bench bench-race check cover

fmt:
	gofmt -w=true -s $$(find . -type f -name '*.go' -not -path "./vendor/*" -not -path "./pb/*")
	goimports -w=true -d $$(find . -type f -name '*.go' -not -path "./vendor/*" -not -path "./pb/*")

test: pb/gostatsd.pb.go
	go test ./...

test-race: pb/gostatsd.pb.go
	go test -race ./...

bench: pb/gostatsd.pb.go
	go test -bench=. -run=XXX ./...

bench-race: pb/gostatsd.pb.go
	go test -race -bench=. -run=XXX ./...

cover: pb/gostatsd.pb.go
	./cover.sh
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out

coveralls: pb/gostatsd.pb.go
	./cover.sh
	goveralls -coverprofile=coverage.out -service=travis-ci

junit-test: build
	go test -v ./... | go-junit-report > test-report.xml

check: pb/gostatsd.pb.go
	go install ./cmd/gostatsd
	go install ./cmd/tester
	golangci-lint run --deadline=600s --enable=gocyclo --enable=dupl \
		--disable=interfacer --disable=golint

check-all: pb/gostatsd.pb.go
	go install ./cmd/gostatsd
	go install ./cmd/tester
	golangci-lint run --deadline=600s --enable=gocyclo --enable=dupl

fuzz-setup:
	go get -v github.com/dvyukov/go-fuzz/go-fuzz
	go get -v github.com/dvyukov/go-fuzz/go-fuzz-build

fuzz:
	go-fuzz-build github.com/atlassian/gostatsd/pkg/statsd
	go-fuzz -bin=./statsd-fuzz.zip -workdir=test_fixtures/lexer_fuzz

watch:
	CompileDaemon -color=true -build "make test"

git-hook:
	cp dev/push-hook.sh .git/hooks/pre-push

# Compile a static binary. Cannot be used with -race
docker: pb/gostatsd.pb.go
	docker pull golang:$(GOVERSION)
	docker run \
		--rm \
		-v "$(GOPATH)":"$(GP)" \
		-w "$(GP)/src/github.com/atlassian/gostatsd" \
		-e GOPATH="$(GP)" \
		-e CGO_ENABLED=0 \
		golang:$(GOVERSION) \
		go build -o build/bin/linux/$(BINARY_NAME) $(GOBUILD_VERSION_ARGS) $(MAIN_PKG)
	docker build --pull -t $(IMAGE_NAME):$(GIT_HASH) build

# Compile a binary with -race. Needs to be run on a glibc-based system.
docker-race: pb/gostatsd.pb.go
	docker pull golang:$(GOVERSION)
	docker run \
		--rm \
		-v "$(GOPATH)":"$(GP)" \
		-w "$(GP)/src/github.com/atlassian/gostatsd" \
		-e GOPATH="$(GP)" \
		golang:$(GOVERSION) \
		go build -race -o build/bin/linux/$(BINARY_NAME) $(GOBUILD_VERSION_ARGS) $(MAIN_PKG)
	docker build --pull -t $(IMAGE_NAME):$(GIT_HASH)-race -f build/Dockerfile-glibc build

# Compile a static binary with symbols. Cannot be used with -race
docker-symbols: pb/gostatsd.pb.go
	docker pull golang:$(GOVERSION)
	docker run \
		--rm \
		-v "$(GOPATH)":"$(GP)" \
		-w "$(GP)/src/github.com/atlassian/gostatsd" \
		-e GOPATH="$(GP)" \
		-e CGO_ENABLED=0 \
		golang:$(GOVERSION) \
		go build -o build/bin/linux/$(BINARY_NAME) $(GOBUILD_VERSION_ARGS_WITH_SYMS) $(MAIN_PKG)
	docker build --pull -t $(IMAGE_NAME):$(GIT_HASH)-syms build

release-hash: docker
	docker push $(IMAGE_NAME):$(GIT_HASH)

release-normal: release-hash
	docker tag $(IMAGE_NAME):$(GIT_HASH) $(IMAGE_NAME):latest
	docker push $(IMAGE_NAME):latest
	docker tag $(IMAGE_NAME):$(GIT_HASH) $(IMAGE_NAME):$(REPO_VERSION)
	docker push $(IMAGE_NAME):$(REPO_VERSION)

release-hash-race: docker-race
	docker push $(IMAGE_NAME):$(GIT_HASH)-race

release-race: docker-race
	docker tag $(IMAGE_NAME):$(GIT_HASH)-race $(IMAGE_NAME):$(REPO_VERSION)-race
	docker push $(IMAGE_NAME):$(REPO_VERSION)-race

release-symbols: docker-symbols
	docker tag $(IMAGE_NAME):$(GIT_HASH)-syms $(IMAGE_NAME):$(REPO_VERSION)-syms
	docker push $(IMAGE_NAME):$(REPO_VERSION)-syms

release: release-normal release-race release-symbols

run: build
	./build/bin/$(ARCH)/$(BINARY_NAME) --backends=stdout --verbose --flush-interval=2s

run-docker: docker
	cd build/ && docker-compose rm -f gostatsd
	docker-compose -f build/docker-compose.yml build
	docker-compose -f build/docker-compose.yml up -d

stop-docker:
	cd build/ && docker-compose stop

version:
	@echo $(REPO_VERSION)

clean:
	rm -f build/bin/*
	-docker rm $(docker ps -a -f 'status=exited' -q)
	-docker rmi $(docker images -f 'dangling=true' -q)

.PHONY: build
