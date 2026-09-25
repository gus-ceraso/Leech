GO ?= go

.PHONY: build test race vet check clean

build:
	CGO_ENABLED=0 $(GO) build -o leech ./cmd/leech

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

check:
	$(MAKE) test
	$(MAKE) race
	$(MAKE) vet
	$(MAKE) build

clean:
	rm -f leech
