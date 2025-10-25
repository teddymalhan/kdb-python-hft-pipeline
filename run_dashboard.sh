#!/bin/bash

# HFT Dashboard Launcher Script
# This script activates the virtual environment and runs the Streamlit dashboard

echo "🚀 Starting HFT Dashboard..."
echo ""

# Activate virtual environment
source /Users/teddymalhan/Documents/python/.venv/bin/activate

# Run Streamlit
cd /Users/teddymalhan/Documents/python/HFT
streamlit run dashboard.py --server.port 8501 --server.headless true

