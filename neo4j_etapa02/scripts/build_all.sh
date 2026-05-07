#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Generando dataset sintético..."
python3 "${SCRIPT_DIR}/generate_dataset.py"

echo "Resumen:"
cat "${ROOT_DIR}/dataset_resumen.txt"
