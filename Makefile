.PHONY: requirements test quality build clean

requirements: ## Install runtime and test/quality dependencies
	pip install -r requirements-test.txt

quality: ## Run style/lint checks
	flake8 panorama_elt tests
	pylint panorama_elt

test: quality ## Run quality checks and the unit test suite
	pytest

build: ## Build the source distribution and wheel
	pip install build
	python -m build

clean: ## Remove build artifacts
	rm -rf build dist *.egg-info
