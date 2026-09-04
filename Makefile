RLM_BIN := bin/gorlm
PRIME_BIN := bin/goprime

.PHONY: fmt vet test build clean

fmt:
	gofmt -w .

vet:
	go vet ./...

test: vet
	go test ./... -race -count=1

build:
	go build -o $(RLM_BIN) ./cmd/gorlm
	go build -o $(PRIME_BIN) ./cmd/goprime

clean:
	rm -rf bin
