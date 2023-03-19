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
CPU_ARCH ?= amd64
MANIFEST_NAME := atlassianlabs/$(BINARY_NAME)
REGISTRY_NAME := docker-public.packages.atlassian.com
IMAGE_NAME := $(REGISTRY_NAME)/$(MANIFEST_NAME)-$(CPU_ARCH)
ARCH ?= $$(uname -s | tr A-Z a-z)
GOVERSION := 1.19.7  # Go version needs to be the same in: CI config, README, Dockerfiles, and Makefile
GP := /gopath
MAIN_PKG := github.com/atlassian/gostatsd/cmd/gostatsd
CLUSTER_PKG := github.com/atlassian/gostatsd/cmd/cluster
PROTOBUF_VERSION ?= 21.5

setup: setup-ci
	go install github.com/githubnemo/CompileDaemon \
		github.com/jstemmer/go-junit-report

tools/bin/protoc:
	curl -L -O https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOBUF_VERSION)/protoc-$(PROTOBUF_VERSION)-linux-x86_64.zip
	unzip -o -d tools/ protoc-$(PROTOBUF_VERSION)-linux-x86_64.zip
	rm protoc-$(PROTOBUF_VERSION)-linux-x86_64.zip
	go install google.golang.org/protobuf/cmd/protoc-gen-go

setup-ci: tools/bin/protoc
	@#go install github.com/golangci/golangci-lint/cmd/golangci-lint

build-cluster:
	go build -i -v -o build/bin/$(ARCH)/cluster $(GOBUILD_VERSION_ARGS) $(CLUSTER_PKG)

pb/gostatsd.pb.go: pb/gostatsd.proto tools/bin/protoc
	GOPATH="tools/bin:${GOPATH}" tools/bin/protoc --go_out=.\
		--go_opt=paths=source_relative $<
	$(RM) protoc-gen-go

build: pb/gostatsd.pb.go
	go build -i -v -o build/bin/$(ARCH)/$(BINARY_NAME) $(GOBUILD_VERSION_ARGS) $(MAIN_PKG)

build-race:
	go build -i -v -race -o build/bin/$(ARCH)/$(BINARY_NAME) $(GOBUILD_VERSION_ARGS) $(MAIN_PKG)

build-all: pb/gostatsd.pb.go
	go install -v ./...

test-all: check-fmt cover test-race bench bench-race check

test-all-full: check-fmt cover test-race-full bench-full bench-race-full check

check-fmt:
	@# Since gofmt and goimports dont return 1 on chamges, this !() stuff will trigger a build failure if theres any problems.
	! (gofmt -l -s $$(find . -type f -name '*.go' -not -path "./vendor/*" -not -path "./pb/*") | grep .)
	! (go run golang.org/x/tools/cmd/goimports -l -local github.com/atlassian/gostatsd $$(find . -type f -name '*.go' -not -path "./vendor/*" -not -path "./pb/*") | grep .)

fix-fmt:
	gofmt -w -s $$(find . -type f -name '*.go' -not -path "./vendor/*" -not -path "./pb/*")
	go run golang.org/x/tools/cmd/goimports -w -l -local github.com/atlassian/gostatsd $$(find . -type f -name '*.go' -not -path "./vendor/*" -not -path "./pb/*")

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

coveralls: pb/gostatsd.pb.go
	./cover.sh
	goveralls -coverprofile=coverage.out -service=travis-ci

junit-test: build
	go test -short -v ./... | go-junit-report > test-report.xml

check: pb/gostatsd.pb.go
	go install ./cmd/gostatsd
	go install ./cmd/tester
	@#golangci-lint run --deadline=600s --enable=gocyclo --enable=dupl --disable=interfacer --disable=golint

check-all: pb/gostatsd.pb.go
	go install ./cmd/gostatsd
	go install ./cmd/tester
	@#golangci-lint run --deadline=600s --enable=gocyclo --enable=dupl

fuzz-setup:
	go install github.com/dvyukov/go-fuzz/go-fuzz github.com/dvyukov/go-fuzz/go-fuzz-build

fuzz:
	go-fuzz-build github.com/atlassian/gostatsd/pkg/statsd
	go-fuzz -bin=./statsd-fuzz.zip -workdir=test_fixtures/lexer_fuzz

watch:
	CompileDaemon -color=true -build "make test"

git-hook:
	cp dev/push-hook.sh .git/hooks/pre-push

build-hash: pb/gostatsd.pb.go
	docker buildx build -t $(IMAGE_NAME):$(GIT_HASH) -f build/Dockerfile-multiarch \
    --build-arg MAIN_PKG=$(MAIN_PKG) \
    --build-arg BINARY_NAME=$(BINARY_NAME) \
    --platform=linux/$(CPU_ARCH) . --load

build-hash-race: pb/gostatsd.pb.go
	docker buildx build -t $(IMAGE_NAME):$(GIT_HASH)-race -f build/Dockerfile-multiarch-glibc \
	--build-arg MAIN_PKG=$(MAIN_PKG) \
	--build-arg BINARY_NAME=$(BINARY_NAME) \
	--platform=linux/$(CPU_ARCH) . --load

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
	for tag in latest $(REPO_VERSION) $(GIT_HASH)-race $(REPO_VERSION)-race; do \
	  for arch in amd64 arm64; do \
		  docker pull $(MANIFEST_NAME)-$$arch:$$tag; \
		done; \
	  docker manifest create $(MANIFEST_NAME):$$tag --amend \
		  $(MANIFEST_NAME)-amd64:$$tag \
		  $(MANIFEST_NAME)-arm64:$$tag; \
	  docker manifest push $(MANIFEST_NAME):$$tag; \
	done

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
