# Makefile for AWS SAM to build Lambda binary
#
# SAM invokes this Makefile inside a Lambda build container

ARCH ?= aarch64-unknown-linux-gnu

.PHONY: build-lambda
build-lambda:
	cargo build --release --target $(ARCH)
	cp ../../target/$(ARCH)/release/bootstrap $(ARTIFACTS_DIR)/bootstrap
