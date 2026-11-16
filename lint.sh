#!/usr/bin/env bash
set -euo pipefail

"${PYTHON:-python}" -m ruff check datacache/ tests/ examples/ "$@"
