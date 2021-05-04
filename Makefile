
GOOS ?= darwin
GOARCH ?= amd64
CGO_ENABLED ?= 1
CGO_CFLAGS ?=
CGO_LDFLAGS ?= "-lrocksdb -lm -lzstd -lz -lbz2 -lsnappy"
VERSION ?=
BIN_EXT ?=
JAVA_CLI_PATH = ./clients/java
CLI_NAME = dkv-client

GO := GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=$(CGO_ENABLED) CGO_CFLAGS=$(CGO_CFLAGS) CGO_LDFLAGS=$(CGO_LDFLAGS) GO111MODULE=on go

PACKAGES = $(shell $(GO) list ./... | grep -v '/vendor/' | grep -v '/extras/')

PROTOBUFS = $(shell find . -name '*.proto' -print0 | xargs -0 -n1 dirname | sort | uniq | grep -v /vendor/ | grep -v /extras/ | grep -v /clients/)

TARGET_PACKAGES = $(shell find . -name 'main.go' -print0 | xargs -0 -n1 dirname | sort | uniq | grep -v /vendor/ | grep -v /extras/)

PROTO_VER = $(shell $(GO) list -m all | grep "github.com/golang/protobuf" | awk '{print $$2}')

ifeq ($(VERSION),)
  VERSION = latest
endif

BUILD_TAGS = 'osusergo netgo static_build'
LDFLAGS = -ldflags '-linkmode external -extldflags "-static" -X "github.com/flipkart-incubator/dkv/version.Version=$(VERSION)"'

ifeq ($(GOOS),darwin)
	LDFLAGS = -ldflags '-X "github.com/flipkart-incubator/dkv/version.Version=$(VERSION)"'
	BUILD_TAGS = 'osusergo netgo'
endif

ifeq ($(GOOS),windows)
  BIN_EXT = .exe
endif


.DEFAULT_GOAL := build

.PHONY: protoc
protoc:
	@echo ">> generating proto code using Proto version $(PROTO_VER)"
	@$(GO) get -u github.com/golang/protobuf/protoc-gen-go@$(PROTO_VER)
	@for proto_dir in $(PROTOBUFS); do echo $$proto_dir; protoc --proto_path=./ --go_out=Mpkg/serverpb/api.proto=github.com/flipkart-incubator/dkv/pkg/serverpb,plugins=grpc,paths=source_relative:. $$proto_dir/*.proto || exit 1; done

.PHONY: format
format:
	@echo ">> formatting code"
	@$(GO) fmt $(PACKAGES)

.PHONY: test
test:
	@echo ">> testing all packages"
	@echo "   GOOS        = $(GOOS)"
	@echo "   GOARCH      = $(GOARCH)"
	@echo "   CGO_ENABLED = $(CGO_ENABLED)"
	@echo "   CGO_CFLAGS  = $(CGO_CFLAGS)"
	@echo "   CGO_LDFLAGS = $(CGO_LDFLAGS)"
	@echo "   BUILD_TAGS  = $(BUILD_TAGS)"
	@$(GO) test -v --count=1 -tags="$(BUILD_TAGS)" $(LDFLAGS) $(PACKAGES)

.PHONY: bench
bench:
	@echo ">> running benchmarks from all packages"
	@echo "   GOOS        = $(GOOS)"
	@echo "   GOARCH      = $(GOARCH)"
	@echo "   CGO_ENABLED = $(CGO_ENABLED)"
	@echo "   CGO_CFLAGS  = $(CGO_CFLAGS)"
	@echo "   CGO_LDFLAGS = $(CGO_LDFLAGS)"
	@echo "   BUILD_TAGS  = $(BUILD_TAGS)"
	@$(GO) test -v --run=xxx --bench=. --count=1 -tags="$(BUILD_TAGS)" $(LDFLAGS) $(PACKAGES)

.PHONY: build
build:
	@echo ">> building binaries"
	@echo "   GOOS        = $(GOOS)"
	@echo "   GOARCH      = $(GOARCH)"
	@echo "   CGO_ENABLED = $(CGO_ENABLED)"
	@echo "   CGO_CFLAGS  = $(CGO_CFLAGS)"
	@echo "   CGO_LDFLAGS = $(CGO_LDFLAGS)"
	@echo "   BUILD_TAGS  = $(BUILD_TAGS)"
	@echo "   VERSION     = $(VERSION)"
	@for target_pkg in $(TARGET_PACKAGES); do echo $$target_pkg; $(GO) build -tags="$(BUILD_TAGS)" $(LDFLAGS) -o ./bin/`basename $$target_pkg`$(BIN_EXT) $$target_pkg || exit 1; done

.PHONY: install
install:
	@echo ">> installing binaries"
	@echo "   GOOS        = $(GOOS)"
	@echo "   GOARCH      = $(GOARCH)"
	@echo "   CGO_ENABLED = $(CGO_ENABLED)"
	@echo "   CGO_CFLAGS  = $(CGO_CFLAGS)"
	@echo "   CGO_LDFLAGS = $(CGO_LDFLAGS)"
	@echo "   BUILD_TAGS  = $(BUILD_TAGS)"
	@echo "   VERSION     = $(VERSION)"
	@for target_pkg in $(TARGET_PACKAGES); do echo $$target_pkg; $(GO) install -tags="$(BUILD_TAGS)" $(LDFLAGS) $$target_pkg || exit 1; done

.PHONY: dist
dist:
	@echo ">> packaging binaries"
	@echo "   GOOS        = $(GOOS)"
	@echo "   GOARCH      = $(GOARCH)"
	@echo "   CGO_ENABLED = $(CGO_ENABLED)"
	@echo "   CGO_CFLAGS  = $(CGO_CFLAGS)"
	@echo "   CGO_LDFLAGS = $(CGO_LDFLAGS)"
	@echo "   BUILD_TAGS  = $(BUILD_TAGS)"
	@echo "   VERSION     = $(VERSION)"
	mkdir -p ./dist/$(GOOS)-$(GOARCH)/bin
	@for target_pkg in $(TARGET_PACKAGES); do echo $$target_pkg; $(GO) build -tags="$(BUILD_TAGS)" $(LDFLAGS) -o ./dist/$(GOOS)-$(GOARCH)/bin/`basename $$target_pkg`$(BIN_EXT) $$target_pkg || exit 1; done
	(cd ./dist/$(GOOS)-$(GOARCH); tar zcfv ../dkv-${VERSION}.$(GOOS)-$(GOARCH).tar.gz .)

.PHONY: git-tag
git-tag:
	@echo ">> tagging github"
	@echo "   VERSION = $(VERSION)"
ifeq ($(VERSION),$(filter $(VERSION),latest master ""))
	@echo "please specify VERSION"
else
	git tag -a $(VERSION) -m "Release $(VERSION)"
	git push origin $(VERSION)
endif

.PHONY: clean
clean:
	@echo ">> cleaning binaries"
	rm -rf ./bin
	rm -rf ./data
	rm -rf ./dist
	rm -rf ${JAVA_CLI_PATH}/${CLI_NAME}/target

.PHONY: java-client
java-client:
	(cd ${JAVA_CLI_PATH}/${CLI_NAME}; mvn clean install -DskipTests)
