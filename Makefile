
build:
	@go build -o bin/gostore ./cmd/server

lint:
	@golangci-lint run

run: build
	@./bin/gostore

test:
	@go test ./... -v