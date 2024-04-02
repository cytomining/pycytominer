install: .devcontainer/postCreateCommand.sh ## Install the poetry environment and install the pre-commit hooks
	@echo "🛠️ Creating virtual environment using poetry and installing pre-commit hooks"
	@bash .devcontainer/postCreateCommand.sh
	@echo "🐢 Launching poetry shell"
	@poetry shell

.PHONY: check
check: ## Run code quality tools.
	@echo "🔒 Checking Poetry lock file consistency with 'pyproject.toml': Running poetry lock --check"
	@poetry check --lock
	@echo "🔎 Linting code: Running pre-commit"
	@poetry run pre-commit run -a

.PHONY: test
test: ## Test the code with pytest
	@echo "🧪 Testing code: Running pytest"
	@poetry run pytest --cov --cov-config=pyproject.toml --cov-report=xml

.PHONY: docs
docs: ## Build the documentation
	@echo "📚 Building documentation"
	@poetry run sphinx-build docs build

.PHONY: build
build: clean-build ## Build wheel file using poetry
	@echo "🛞 Creating wheel and sdist files"
	@poetry build

.PHONY: clean-build
clean-build: ## clean build artifacts
	@echo "🧹 Cleaning build artifacts"
	@rm -rf dist

.PHONY: docker-build
test_docker_build: ## Build the docker image and run the tests
	@echo "🐳 Building docker image and running tests"
	@docker build -f build/docker/Dockerfile -t pycytominer:latest .
	@docker run pycytominer:latest poetry run pytest

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
