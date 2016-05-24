VERSION_VAR := main.Version
GIT_VAR := main.GitCommit
BUILD_DATE_VAR := main.BuildDate
REPO_VERSION := $$(git describe --abbrev=0 --tags)
BUILD_DATE := $$(date +%Y-%m-%d-%H:%M)
GIT_HASH := $$(git rev-parse --short HEAD)
GOBUILD_VERSION_ARGS := -ldflags "-s -X $(VERSION_VAR)=$(REPO_VERSION) -X $(GIT_VAR)=$(GIT_HASH) -X $(BUILD_DATE_VAR)=$(BUILD_DATE)"
BINARY_NAME := gostatsd
IMAGE_NAME := atlassianlabs/$(BINARY_NAME)
ARCH ?= darwin
METALINTER_CONCURRENCY ?= 4

setup:
	go get -v -u github.com/Masterminds/glide
	go get -v -u github.com/githubnemo/CompileDaemon
	go get -v -u github.com/alecthomas/gometalinter
	go get -v -u github.com/jstemmer/go-junit-report
	gometalinter --install --update
	glide install

build: *.go fmt
	go build -o build/bin/$(ARCH)/$(BINARY_NAME) $(GOBUILD_VERSION_ARGS) github.com/atlassian/gostatsd

build-race: *.go fmt
	go build -race -o build/bin/$(ARCH)/$(BINARY_NAME) $(GOBUILD_VERSION_ARGS) github.com/atlassian/gostatsd

build-all:
	go build $$(glide nv)

fmt:
	gofmt -w=true -s $$(find . -type f -name '*.go' -not -path "./vendor/*")
	goimports -w=true -d $$(find . -type f -name '*.go' -not -path "./vendor/*")

test:
	go test $$(glide nv)

test-race:
	go test -race $$(glide nv)

bench:
	go test -bench=. $$(glide nv)

bench-race:
	go test -race -bench=. $$(glide nv)

cover:
	./cover.sh
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out

coveralls:
	./cover.sh
	goveralls -coverprofile=coverage.out -service=travis-ci

junit-test: build
	go test -v $$(glide nv) | go-junit-report > test-report.xml

check:
	go install
	go install ./tester
	gometalinter --concurrency=$(METALINTER_CONCURRENCY) --deadline=180s ./... --vendor --linter='errcheck:errcheck:-ignore=net:Close' --cyclo-over=20 \
		--linter='vet:go tool vet -composites=false {paths}:PATH:LINE:MESSAGE' --disable=interfacer --dupl-threshold=200

check-all:
	go install
	go install ./tester
	gometalinter --concurrency=$(METALINTER_CONCURRENCY) --deadline=600s ./... --vendor --cyclo-over=20 \
		--linter='vet:go tool vet {paths}:PATH:LINE:MESSAGE' --dupl-threshold=65

profile:
	./build/bin/$(ARCH)/$(BINARY_NAME) --backends=stdout --cpu-profile=./profile.out --flush-interval=1s
	go tool pprof build/bin/$(ARCH)/$(BINARY_NAME) profile.out

watch:
	CompileDaemon -color=true -build "make test"

git-hook:
	cp dev/push-hook.sh .git/hooks/pre-push

cross:
	CGO_ENABLED=0 GOOS=linux go build -o build/bin/linux/$(BINARY_NAME) $(GOBUILD_VERSION_ARGS) -a -installsuffix cgo  github.com/atlassian/$(BINARY_NAME)

docker: cross
	cd build && docker build -t $(IMAGE_NAME):$(GIT_HASH) .

release-hash: check test docker
	docker push $(IMAGE_NAME):$(GIT_HASH)

release: check test docker release-hash
	docker tag $(IMAGE_NAME):$(GIT_HASH) $(IMAGE_NAME):latest
	docker push $(IMAGE_NAME):latest
	docker tag $(IMAGE_NAME):$(GIT_HASH) $(IMAGE_NAME):$(REPO_VERSION)
	docker push $(IMAGE_NAME):$(REPO_VERSION)

run: build
	./build/bin/$(ARCH)/$(BINARY_NAME) --backends=stdout --verbose --flush-interval=1s

run-docker: cross
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
