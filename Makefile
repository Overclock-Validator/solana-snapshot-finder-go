## Makefile - common developer commands

.PHONY: fmt vet lint test build tidy

fmt:
	gofmt -s -w .

fmt-check:
	@if [ -n "$(gofmt -l .)" ]; then echo "Files need formatting:" && gofmt -l . && exit 1; fi

## Developer tool installation notes:
## To install golangci-lint locally (preferred):
## go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.58.0
## If that fails on macOS/arm64, you can run:
## curl -sSfL https://github.com/golangci/golangci-lint/releases/download/v1.58.0/golangci-lint-1.58.0-darwin-arm64.tar.gz \
## | sudo tar -xz -C /usr/local/bin --strip-components=1


ci: fmt-check vet lint tidy test
	@echo "CI checks passed"

vet:
	go vet ./...

lint:
	golangci-lint run

test:
	go test ./... -v

build:
	go build ./...

tidy:
	go mod tidy
