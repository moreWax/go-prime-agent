BIN := bin/gorlm

.PHONY: fmt vet test build clean

fmt:
	gofmt -w .

vet:
	go vet ./...

test: vet
	go test ./... -race -count=1

build:
	go build -o $(BIN) ./cmd/gorlm

clean:
	rm -rf bin
