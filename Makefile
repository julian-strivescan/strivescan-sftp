# Build commands
build:
	go build -o bin/strivescan-sftp cmd/strivescan-sftp/main.go

# Run commands
run:
	./bin/strivescan-sftp

# Build and run in one command
dev: build run

# Clean build artifacts
clean:
	rm -rf bin/

.PHONY: build run dev clean
