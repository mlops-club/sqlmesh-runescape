export PYTHONPATH = .venv

.PHONY: setup
setup:
	@if [ ! -f "uv.lock" ]; then \
		echo "Can't find lockfile. Locking"; \
		uv lock; \
	fi
	uv sync --all-extras
	uv pip install --no-deps -e .

.PHONY: format
format: setup
	uv --quiet run ruff format .
	uv --quiet run ruff check --fix .

.PHONY: lint
lint: setup
	uv --quiet run ruff check .
	uv --quiet run ruff format --check
	uv --quiet run mypy .

.PHONY: test
test: setup
	uv --quiet run pytest tests --cov=api --cov=tests --cov-fail-under=85 --cov-branch tests