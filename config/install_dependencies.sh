#!/usr/bin/env bash
set -Eeuo pipefail

if ! command -v uv >/dev/null 2>&1; then
    printf '%s\n' 'uv is required. Install it before running this script.' >&2
    exit 1
fi

printf '%s\n' 'Installing Xephyr...'
sudo apt-get update
sudo env DEBIAN_FRONTEND=noninteractive apt-get install \
    -y \
    --no-install-recommends \
    xserver-xephyr

printf '%s\n' 'Syncing locked runtime dependencies...'
uv sync --locked --no-dev

printf '%s\n' 'Installing Playwright Chromium and OS dependencies...'
uv run --locked --no-dev playwright install --with-deps chromium
