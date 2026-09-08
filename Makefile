.PHONY: build run run5 test bench-load clean

build:
	go build -o bin/quorumlogd ./cmd/quorumlogd
	go build -o bin/qlbench ./cmd/qlbench

# One-command local 3-node cluster on 127.0.0.1:9101-9103.
run:
	bash scripts/run-cluster.sh 3

# 5-node cluster on 127.0.0.1:9101-9105.
run5:
	bash scripts/run-cluster.sh 5

test:
	go test ./...

clean:
	rm -rf bin data
