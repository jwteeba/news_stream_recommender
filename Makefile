.PHONY: test test-unit test-integration test-coverage test-fast install-test-deps clean-test

install-test-deps:
	poetry install

test: install-test-deps
	pytest --verbose

test-coverage: install-test-deps
	pytest --coverage --verbose

test-backend: install-test-deps
	pytest tests/test_backend.py --verbose

test-frontend: install-test-deps
	pytest tests/test_frontend.py --verbose

test-kafka: install-test-deps
	pytest tests/test_kafka_producer.py --verbose

test-spark: install-test-deps
	pytest tests/test_spark_streaming.py  --verbose

# Clean test artifacts
clean-test:
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Run linting (if available)
lint:
	@if command -v flake8 >/dev/null 2>&1; then \
		echo "Running flake8..."; \
		flake8 src/ tests/; \
	else \
		echo "flake8 not found, skipping linting"; \
	fi

test-all: clean-test lint test-coverage

# Help
help:
	@echo "Available targets:"
	@echo "  test              - Run all tests"
	@echo "  test-coverage     - Run tests with coverage report"
	@echo "  test-backend      - Run backend tests only"
	@echo "  test-frontend     - Run frontend tests only"
	@echo "  test-kafka        - Run Kafka producer tests only"
	@echo "  test-spark        - Run Spark streaming tests only"
	@echo "  clean-test        - Clean test artifacts"
	@echo "  lint              - Run code linting"
	@echo "  install-test-deps - Install test dependencies"