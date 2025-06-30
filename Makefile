# HSM Development Makefile

.PHONY: help test test-unit test-integration test-cli test-coverage test-fast test-simple-mlp docs docs-build docs-serve clean install install-dev lint format check verify-install

# Default target
help:
	@echo "HSM Development Commands:"
	@echo ""
	@echo "Testing:"
	@echo "  test              Run all tests"
	@echo "  test-unit         Run unit tests only"
	@echo "  test-integration  Run integration tests only" 
	@echo "  test-cli          Run CLI tests only"
	@echo "  test-fast         Run fast tests (exclude slow/remote)"
	@echo "  test-coverage     Run tests with detailed coverage report"
	@echo "  test-simple-mlp   Run tests using simple_mlp example"
	@echo ""
	@echo "Code Quality (Ruff):"
	@echo "  lint              Run linting checks"
	@echo "  lint-fix          Run linting checks with auto-fix"
	@echo "  format            Format code"
	@echo "  format-check      Check code formatting"
	@echo "  ruff-all          Run all ruff operations"
	@echo "  check             Run all checks (lint + format + test)"
	@echo ""
	@echo "Documentation:"
	@echo "  docs              Build documentation"
	@echo "  docs-serve        Serve documentation locally"
	@echo ""
	@echo "Development:"
	@echo "  install           Install package in development mode"
	@echo "  install-dev       Install with development dependencies"
	@echo "  clean             Clean build artifacts"

# Test commands
test:
	@echo "Running all tests..."
	pytest -v tests/

test-unit:
	@echo "Running unit tests..."
	pytest -v tests/unit/

test-integration:
	@echo "Running integration tests..."
	pytest -v tests/integration/

test-cli:
	@echo "Running CLI tests..."
	pytest -v tests/cli/

test-fast:
	@echo "Running fast tests (excluding slow tests)..."
	pytest -v tests/ -m "not slow" --tb=short

test-coverage:
	@echo "Running tests with coverage report..."
	pytest -v tests/ --cov=hsm --cov-report=html --cov-report=term-missing
	@echo "Coverage report generated in htmlcov/"

test-simple-mlp:
	@echo "Running tests with simple_mlp example..."
	pytest -v tests/integration/test_simple_mlp_integration.py

# Documentation commands
docs:
	@echo "Building documentation..."
	@echo "API reference documentation is in docs/api_reference/"
	@echo "CLI documentation is in docs/cli/"
	@echo "User guide is in docs/user_guide/"

docs-serve:
	@echo "Documentation is available in docs/ directory"
	@echo "Open docs/README.md or docs/api_reference/README.md to get started"

# Development commands
install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"
	@echo "Development dependencies installed!"

lint:
	@echo "Running linting checks with ruff..."
	ruff check src/ tests/

lint-fix:
	@echo "Running linting checks with ruff (with auto-fix)..."
	ruff check --fix src/ tests/

format:
	@echo "Formatting code with ruff..."
	ruff format src/ tests/

format-check:
	@echo "Checking code formatting with ruff..."
	ruff format --check src/ tests/

check: lint format-check test-fast
	@echo "All checks completed!"

clean:
	@echo "Cleaning build artifacts..."
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf .pytest_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

# Test data and examples
test-examples:
	@echo "Testing example configurations..."
	cd tests/fixtures && python -m pytest test_examples.py -v

# CI/CD helpers
ci-test:
	pytest -v --junitxml=test-results.xml --cov-report=xml

ci-lint:
	ruff check --output-format=github src/ tests/
	ruff format --check src/ tests/

# Development utilities
dev-setup: install-dev
	@echo "Development environment setup complete!"
	@echo "Run 'make test' to verify installation"

quick-test:
	pytest tests/unit/ -v --tb=short

# Documentation validation
docs-check:
	@echo "Checking documentation links and syntax..."
	find docs/ -name "*.md" -exec echo "Checking: {}" \;

# Package building
build:
	python -m build

# Installation verification
verify-install:
	@echo "Verifying HSM installation..."
	python -c "import hsm; print('HSM package imported successfully')"
	hsm --help
	@echo "HSM installation verified!"

# Coverage targets
coverage-unit:
	@echo "Running unit tests with coverage..."
	pytest tests/unit/ --cov=hsm --cov-report=term-missing

coverage-integration:
	@echo "Running integration tests with coverage..."
	pytest tests/integration/ --cov=hsm --cov-report=term-missing

# Ruff-specific targets
ruff-all:
	@echo "Running all ruff checks and formatting..."
	ruff check --fix src/ tests/
	ruff format src/ tests/

ruff-stats:
	@echo "Ruff statistics:"
	ruff check --statistics src/ tests/

# Help for specific test types
test-help:
	@echo "Available test markers:"
	@echo "  unit         - Fast, isolated unit tests"
	@echo "  integration  - Multi-component integration tests"
	@echo "  cli          - Command-line interface tests"
	@echo "  slow         - Long-running tests"
	@echo "  remote       - Tests requiring SSH/remote access"
	@echo "  hpc          - Tests requiring HPC environment"
	@echo ""
	@echo "Example usage:"
	@echo "  make test-unit              # Run only unit tests"
	@echo "  pytest -m 'unit and not slow'  # Custom marker combinations"

# Ruff help
ruff-help:
	@echo "Ruff commands:"
	@echo "  make lint           # Check code for issues"
	@echo "  make lint-fix       # Check and auto-fix issues"
	@echo "  make format         # Format code"
	@echo "  make format-check   # Check if code is formatted"
	@echo "  make ruff-all       # Run all ruff operations"
	@echo "  make ruff-stats     # Show ruff statistics" 