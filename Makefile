SHELL=bash
S3_READY_REGEX=^Ready\.$
aws_dev_account=NOT_SET
temp_image_name=NOT_SET
aws_default_region=NOT_SET

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: bootstrap
bootstrap: ## Bootstrap local environment for first use
	make git-hooks

.PHONY: git-hooks
git-hooks: ## Set up hooks in .git/hooks
	@{ \
		HOOK_DIR=.git/hooks; \
		for hook in $(shell ls .githooks); do \
			if [ ! -h $${HOOK_DIR}/$${hook} -a -x $${HOOK_DIR}/$${hook} ]; then \
				mv $${HOOK_DIR}/$${hook} $${HOOK_DIR}/$${hook}.local; \
				echo "moved existing $${hook} to $${hook}.local"; \
			fi; \
			ln -s -f ../../.githooks/$${hook} $${HOOK_DIR}/$${hook}; \
		done \
	}

local-build: ## Build Kafka2Hbase with gradle
	gradle :unit build -x test

local-dist: ## Assemble distribution files in build/dist with gradle
	gradle assembleDist

local-test: ## Run the unit tests with gradle
	gradle --rerun-tasks unit

local-all: local-build local-test local-dist ## Build and test with gradle

integration-test: ## Run the integration tests in a Docker container
	echo "WIP"

integration-test-equality: ## Run the integration tests in a Docker container
	echo "WIP"

integration-load-test: ## Run the integration load tests in a Docker container
	echo "WIP"

.PHONY: integration-all ## Build and Run all the tests in containers from a clean start
integration-all:
	echo "WIP"
