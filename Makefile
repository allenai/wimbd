NGRAM = 3
WORKERS = 4
DIR = ./bin

CRATE_VERSION := $(shell cargo metadata --format-version=1 --no-deps | jq -r '.packages[0].version')
ifndef CRATE_VERSION
$(error failed to parse cargo package version)
endif

GIT_TAG := $(shell git describe --always)
ifndef GIT_TAG
$(error failed to parse git tag/commit)
endif

BUILD_VERSION := v$(CRATE_VERSION), $(GIT_TAG)

.PHONY : release
release :
	BUILD_VERSION="$(BUILD_VERSION)" cargo build --release
	mkdir -p $(DIR)
	cp target/release/wimbd $(DIR)/wimbd

.PHONY : benchmark
benchmark :
	cargo run --release -- topk \
		/mnt/tank/c4/en/c4-train.01009-of-01024.json.gz \
		/mnt/tank/c4/en/c4-train.01010-of-01024.json.gz \
		/mnt/tank/c4/en/c4-train.01011-of-01024.json.gz \
		/mnt/tank/c4/en/c4-train.01012-of-01024.json.gz \
		/mnt/tank/c4/en/c4-train.01013-of-01024.json.gz \
		--ngram $(NGRAM) \
		--workers $(WORKERS) \
		--limit 100000

.PHONY : lint
lint :
	cargo clippy --all-targets -- -D warnings
