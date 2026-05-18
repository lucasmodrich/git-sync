BINARY  := git-sync
MODULE  := github.com/AkashRajpurohit/git-sync
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "devel")
BUILD   := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w \
	-X $(MODULE)/pkg/version.Version=$(VERSION) \
	-X $(MODULE)/pkg/version.Build=$(BUILD)

.PHONY: all build run test lint clean

all: build

build:
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BINARY) .

run:
	go run main.go

test:
	go test -v ./...

lint:
	go vet ./...

clean:
	rm -f $(BINARY)
