#!/bin/bash
echo "Updating package lists..."
sudo apt-get update

echo "Installing required Linux packages..."
sudo apt-get install -y \
        xserver-xephyr

echo "Installing Python packages..."
if command -v uv &> /dev/null; then
    echo "uv found. Syncing dependencies..."
    uv sync
    
    echo "Installing Playwright browsers..."
    uv run playwright install --with-deps chromium
else
    echo "Error: uv is not installed. Please install uv first."
    echo "You can install it by running: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

echo "All dependencies installed successfully."