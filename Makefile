.PHONY: help build docs check test numpydoc mypy all

MAKEFLAGS += --no-print-directory

help:
	@echo "Available targets:"
	@echo "  build    - Build the package"
	@echo "  deps     - Install dependencies"
	@echo "  check    - Run pre-commit checks"
	@echo "  test     - Run tests"
	@echo "  numpydoc - Check numpydoc style"
	@echo "  mypy     - Run mypy static type checker"
	@echo "  all      - Run all tasks"

build check test numpydoc mypy: print_target

print_target:
	@echo "Executing $(MAKECMDGOALS)..."

build:
	@uv pip install --force-reinstall --no-deps --link-mode=copy -e .

deps:
	@uv pip install --upgrade -r requirements.txt --link-mode=copy

install: deps build

check:
	@pre-commit run --all-files

test:
	@pytest -s --log-level DEBUG

numpydoc:
	@python -m numpydoc.hooks.validate_docstrings $(shell find python -name "*.py" ! -name "cli.py")

mypy:
	@mypy python/

all:
	@$(MAKE) build
	@$(MAKE) deps
	@$(MAKE) check
	@$(MAKE) test
	@$(MAKE) numpydoc
	@$(MAKE) mypy
	@echo "All tasks completed."
