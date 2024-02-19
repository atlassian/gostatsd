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
GOVERSION           := 1.21.4  # Go version needs to be the same in: CI config, README, Dockerfiles, and Makefile
GP                  := /gopath
MAIN_PKG            := github.com/atlassian/gostatsd/cmd/gostatsd
CLUSTER_PKG         := github.com/atlassian/gostatsd/cmd/cluster
LAMBDA_PKG          := github.com/atlassian/gostatsd/cmd/lambda-extension
PROTOBUF_VERSION    ?= 21.5
PROJECT_ROOT_DIR    := $(realpath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
TOOLS_DIR           := $(PROJECT_ROOT_DIR)/.tools
TOOLS_SRC_DIR       := $(PROJECT_ROOT_DIR)/internal/tools
ALL_TOOLS_PACKAGES  := $(shell grep -E '(^|\s)_\s+\".*\"$$' < $(TOOLS_SRC_DIR)/tools.go | tr -d '"' | awk '{print $$2;}')
ALL_TOOLS_COMMAND   := $(sort $(addprefix $(TOOLS_DIR)/,$(notdir $(ALL_TOOLS_PACKAGES))))

.PHONY: tools
tools: $(ALL_TOOLS_COMMAND)

$(ALL_TOOLS_COMMAND): $(TOOLS_DIR) $(TOOLS_SRC_DIR)/go.mod
	cd $(TOOLS_SRC_DIR) && CGO_ENABLED=0 go build -o $(TOOLS_DIR)/$(notdir $@) $(filter %/$(notdir $@),$(ALL_TOOLS_PACKAGES))

$(TOOLS_DIR):
	mkdir -p $(TOOLS_DIR)

tools/bin/protoc:
	curl -L -O https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOBUF_VERSION)/protoc-$(PROTOBUF_VERSION)-linux-x86_64.zip
	unzip -o -d tools/ protoc-$(PROTOBUF_VERSION)-linux-x86_64.zip
	rm protoc-$(PROTOBUF_VERSION)-linux-x86_64.zip

pb/gostatsd.pb.go: pb/gostatsd.proto tools/bin/protoc
	GOPATH="tools/bin:${GOPATH}" tools/bin/protoc --go_out=.\
		--go_opt=paths=source_relative $<

build: pb/gostatsd.pb.go
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

build-all: pb/gostatsd.pb.go tools

test-all: check-fmt cover test-race bench bench-race check

test-all-full: check-fmt cover test-race-full bench-full bench-race-full check

check-fmt: $(TOOLS_DIR)/goimports
	@# Since gofmt and goimports dont return 1 on chamges, this !() stuff will trigger a build failure if theres any problems.
	! (gofmt -l -s $$(find . -type f -name '*.go' -not -path "./vendor/*" -not -path "./pb/*") | grep .)
	! ($(TOOLS_DIR)/goimports -l -local github.com/atlassian/gostatsd $$(find . -type f -name '*.go' -not -path "./vendor/*" -not -path "./pb/*") | grep .)

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

coveralls: $(TOOLS_DIR)/goveralls pb/gostatsd.pb.go
	./cover.sh
	$(TOOLS_DIR)/goveralls -coverprofile=coverage.out -service=travis-ci

junit-test: build
	go test -short -v ./... | go-junit-report > test-report.xml

check: pb/gostatsd.pb.go
	go install ./cmd/gostatsd
	go install ./cmd/tester

check-all: pb/gostatsd.pb.go
	go install ./cmd/gostatsd
	go install ./cmd/tester

fuzz: $(TOOLS_DIR)/go-fuzz-build $(TOOLS_DIR)/go-fuzz
	$(TOOLS_DIR)/go-fuzz-build github.com/atlassian/gostatsd/pkg/statsd
	$(TOOLS_DIR)/go-fuzz -bin=./statsd-fuzz.zip -workdir=test_fixtures/lexer_fuzz

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
	rm -rf build/bin/*
	-docker rm $(docker ps -a -f 'status=exited' -q)
	-docker rmi $(docker images -f 'dangling=true' -q)

.PHONY: build
