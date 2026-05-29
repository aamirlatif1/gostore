build:
	@go build -o bin/gostore

run: build
	@./bin/gostore

test:
	@go test ./... -v