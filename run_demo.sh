#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "============================================================"
echo "⚡ Starting Google Cloud Pub/Sub Anthropic Architecture Demo"
echo "============================================================"

# Ensure venv exists
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
    source .venv/bin/activate
    uv pip install --default-index https://pypi.org/simple -r requirements.txt
else
    source .venv/bin/activate
fi

# Compile protobuf schema
echo "Compiling Protocol Buffers schema..."
python3 scripts/compile_proto.py

echo "Launching Streamlit Web Dashboard..."
echo "Open your browser at: http://localhost:8501"
export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH}"
exec streamlit run src/dashboard.py --server.port=8501 --server.address=0.0.0.0
