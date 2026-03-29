.PHONY: web build test

build:
	cd web && npm run build

web: build
	uv run main.py web

test:
	uv run pytest -m 'not llm'
