# Contributing

## Set up the repository

```bash
uv sync --locked
```

The service requires Python 3.11 or newer. A live headful run also requires Playwright Chromium and Xephyr; unit and ASGI tests do not start those resources.

## Make one coherent change

Keep a change focused on one observable outcome. Include the tests and documentation needed to prove that outcome, but separate unrelated formatting, dependency upgrades, and behavior changes.

Use domain-specific names and keep failure behavior explicit. Do not hide errors with broad exception handling unless the code is at a cleanup or process boundary where best-effort teardown is intentional; log that boundary failure with enough context to diagnose it.

## Validate before review

```bash
uv run ruff check src tests example
uv run ruff format --check src tests example
uv run pytest -q
uv build
```

For runtime changes, also verify the lightweight import path:

```bash
RUN_HEAVY_STARTUP=false uv run python -c "import src.main; print(src.main.app.title)"
```

## Describe the change

Use a concise commit or pull-request title that can stand as the final Git history entry, for example:

```text
fix(jobs): preserve uploaded file cleanup on cancellation
refactor(worker): separate subprocess outcome parsing
docs: align local validation commands
```

In the description, record why the change was needed, what behavior changed, and the exact checks that passed. A task template or long checklist is not required.
