install: .devcontainer/postCreateCommand.sh ## Install the uv environment and install the pre-commit hooks
	@echo "🛠️ Creating virtual environment using uv and installing pre-commit hooks"
	@bash .devcontainer/postCreateCommand.sh
	@echo "🐢 Environment ready. Activate with 'source .venv/bin/activate' or use 'uv run <command>'."

.PHONY: check
check: ## Run code quality tools.
	@echo "🔒 Checking uv lock file consistency with 'pyproject.toml': Running uv lock --check"
	@uv lock --check
	@echo "🔎 Linting code: Running pre-commit"
	@uv run pre-commit run -a

.PHONY: test
test: ## Test the code with pytest
	@echo "🧪 Testing code: Running pytest"
	@uv run pytest --cov --cov-config=pyproject.toml --cov-report=xml

.PHONY: docs
docs: ## Build the documentation
	@echo "📚 Building documentation"
	@uv run sphinx-build docs build

.PHONY: build
build: clean-build ## Build wheel file using uv
	@echo "🛞 Creating wheel and sdist files"
	@uv build

.PHONY: clean-build
clean-build: ## clean build artifacts
	@echo "🧹 Cleaning build artifacts"
	@rm -rf dist

.PHONY: docker-build
test_docker_build: ## Build the docker image and run the tests
	@echo "🐳 Building docker image and running tests"
	@docker build -f build/docker/Dockerfile -t pycytominer:latest .
	@docker run pycytominer:latest pytest

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' "$(MAKEFILE_LIST)" | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
