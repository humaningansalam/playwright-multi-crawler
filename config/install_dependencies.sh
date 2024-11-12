#!/bin/bash
echo "Updating package lists..."
sudo apt-get update

echo "Installing required Linux packages..."
sudo apt-get install -y \
        xserver-xephyr

echo "Installing Python packages..."
if command -v poetry &> /dev/null; then
    poetry install
    poetry run playwright install-deps
    poetry run playwright install chromium 
else
    echo "Error: Poetry is not installed. Please install Poetry first."
    exit 1
fi

echo "All dependencies installed successfully."