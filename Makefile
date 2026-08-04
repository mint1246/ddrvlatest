BINARY_NAME=ddrv

build-web:
	npm --prefix web ci
	npm --prefix web run build

build: build-web
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
	npm --prefix web test

fmt:
	cargo fmt
