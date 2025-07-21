#!/bin/bash

echo "-------------------------------"
echo "🧙 Annie’s Magic Numbers Setup"
echo "-------------------------------"

# 1. Check Python
if ! command -v python3 &> /dev/null; then
  echo "❌ Python3 not found. Attempting to install..."

  if [ "$(uname)" == "Darwin" ]; then
    # macOS
    if ! command -v brew &> /dev/null; then
      echo "❌ Homebrew is required but not installed. Install it from https://brew.sh"
      exit 1
    fi
    brew install python
  elif [ -f /etc/debian_version ]; then
    # Debian/Ubuntu
    sudo apt update
    sudo apt install -y python3 python3-venv python3-pip
  elif [ -f /etc/redhat-release ]; then
    # RHEL/Fedora/CentOS
    sudo yum install -y python3
  else
    echo "❌ Unsupported OS. Please install Python 3 manually."
    exit 1
  fi
else
  echo "✅ Python3 is already installed."
fi

# 2. Create virtual environment
echo "🌀 Creating virtual environment..."
python3 -m venv env

if [ ! -f env/bin/activate ]; then
  echo "❌ Failed to create virtual environment."
  exit 1
fi

# 3. Activate environment
source env/bin/activate

# 4. Upgrade pip and install dependencies
echo "📦 Installing required packages..."
pip install --upgrade pip
pip install -r requirements.txt

if [ $? -ne 0 ]; then
  echo "❌ Failed to install some packages."
  exit 1
fi

echo "🎉 Setup complete!"
echo "To activate the environment later, run:"
echo "  source env/bin/activate"

exec bash
