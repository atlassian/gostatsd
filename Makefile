VERSION_VAR := main.Version
GIT_VAR := main.GitCommit
BUILD_DATE_VAR := main.BuildDate
REPO_VERSION := $(shell git describe --abbrev=0 --tags)
BUILD_DATE := $(shell date +%Y-%m-%d-%H:%M)
GIT_HASH := $(shell git rev-parse --short HEAD)
GOBUILD_VERSION_ARGS := -ldflags "-s -X $(VERSION_VAR)=$(REPO_VERSION) -X $(GIT_VAR)=$(GIT_HASH) -X $(BUILD_DATE_VAR)=$(BUILD_DATE)"
BINARY_NAME := gostatsd
IMAGE_NAME := jtblin/$(BINARY_NAME)
ARCH ?= darwin

setup:
	go get -v -u github.com/githubnemo/CompileDaemon
	go get -v -u github.com/alecthomas/gometalinter
	gometalinter --install --update
	GO15VENDOREXPERIMENT=1 glide install

build: *.go fmt
	go build -o build/bin/$(ARCH)/$(BINARY_NAME) $(GOBUILD_VERSION_ARGS) github.com/jtblin/$(BINARY_NAME)

fmt:
	gofmt -w=true -s $(shell find . -type f -name '*.go' -not -path "./vendor/*")
	goimports -w=true -d $(shell find . -type f -name '*.go' -not -path "./vendor/*")

test:
	GO15VENDOREXPERIMENT=1 go test $(shell GO15VENDOREXPERIMENT=1 go list ./... | grep -v /vendor/)

cover:
	GO15VENDOREXPERIMENT=1 go test -covermode=count -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out

junit-test: build
	go get github.com/jstemmer/go-junit-report
	go test -v ./... | go-junit-report > test-report.xml

check:
	gometalinter --deadline=10s ./... --vendor

watch:
	CompileDaemon -color=true -build "make test check"

commit-hook:
	cp dev/commit-hook.sh .git/hooks/pre-commit

cross:
	CGO_ENABLED=0 GOOS=linux go build -o build/bin/linux/$(BINARY_NAME) $(GOBUILD_VERSION_ARGS) -a -installsuffix cgo  github.com/jtblin/$(BINARY_NAME)

docker: cross
	cd build && docker build -t $(IMAGE_NAME):$(GIT_HASH) .

release: test docker
	docker push $(IMAGE_NAME):$(GIT_HASH)
	docker tag -f $(IMAGE_NAME):$(GIT_HASH) $(IMAGE_NAME):latest
	docker push $(IMAGE_NAME):latest
	docker tag -f $(IMAGE_NAME):$(GIT_HASH) $(IMAGE_NAME):$(REPO_VERSION)
	docker push $(IMAGE_NAME):$(REPO_VERSION)

run: build
	./build/bin/$(ARCH)/$(BINARY_NAME) --backends=stdout --verbose --flush-interval=10s

run-docker: cross
	cd build/ && docker-compose rm -f
	docker-compose -f build/docker-compose.yml build
	docker-compose -f build/docker-compose.yml up --force-recreate

version:
	@echo $(REPO_VERSION)

clean:
	rm -f build/bin/*
	-docker rm $(docker ps -a -f 'status=exited' -q)
	-docker rmi $(docker images -f 'dangling=true' -q)

.PHONY: build
