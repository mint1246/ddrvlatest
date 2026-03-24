BINARY_NAME=ddrv

build:
	cargo build --release
	cp target/release/$(BINARY_NAME) ./$(BINARY_NAME)

build-debug:
	cargo build
	cp target/debug/$(BINARY_NAME) ./$(BINARY_NAME)

build-docker:
	cargo build --release
	cp target/release/$(BINARY_NAME) ./$(BINARY_NAME)

build-image:
	docker build -t ddrv:latest .

clean:
	cargo clean
	rm -f $(BINARY_NAME)

test:
	cargo test

fmt:
	cargo fmt
