.PHONY: format test

format:
	uv run ruff format .
	uv run ruff check --fix .

test:
	uv run pytest -v --cov=src --cov-report=html
