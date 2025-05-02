# Build commands
build:
	go build -o bin/strivescan-sftp cmd/strivescan-sftp/main.go

# Run commands
run:
	./bin/strivescan-sftp -scan-type all -days 3 -team 1

# Build and run in one command
dev: build run

# Clean build artifacts
clean:
	rm -rf bin/ output/

.PHONY: build run dev clean
