.PHONY: web build test images

build:
	cd web && npm run build

images:
	@for f in wf-images/*.Dockerfile; do \
		name=$$(basename "$$f" .Dockerfile); \
		echo "Building turbo-$$name..."; \
		docker build -t "turbo-$$name" -f "$$f" wf-images; \
	done

web: build
	uv run main.py web

test:
	uv run pytest -m 'not llm'
