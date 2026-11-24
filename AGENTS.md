# Repository guide

`playwright-multi-crawler` is a single-node FastAPI service that queues trusted Playwright jobs and runs each job in a subprocess connected to one shared Chromium instance.

## Read map

- `README.md`: operator setup, HTTP/CLI usage, and runtime behavior
- `src/api/`: HTTP boundaries and response contracts
- `src/core/`: queue, durable state, and shared browser lifecycle
- `src/worker/`: subprocess execution and user-script result handling
- `src/models/job.py`: API, worker, and persisted-state models
- `tests/`: contract and failure-path coverage

## Required checks

Run these from the repository root:

```bash
uv sync --locked
uv run ruff check src tests example
uv run ruff format --check src tests example
uv run pytest -q
uv build
```

## Change boundaries

- Preserve documented API routes, status values, structured error codes, and response shapes unless the product contract explicitly changes.
- Keep job state writes atomic and keep terminal states terminal.
- Preserve cancellation cleanup, subprocess-group termination, path-traversal protection, and reserved service filenames.
- Add a focused regression test for every bug fix or observable behavior change.
- Prefer explicit feature-local helpers over generic utility layers or speculative abstractions.
- Use descriptive names that express domain meaning; avoid placeholders such as `data`, `info`, `item`, `temp`, or `manager` when a concrete name is available.

A separate task document is not required for routine changes. Create durable documentation only when it records a product contract, architectural decision, or operating procedure that the code and tests cannot express on their own.
