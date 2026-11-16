#!/usr/bin/env bash
set -euo pipefail

"${PYTHON:-python}" -m pytest --cov=datacache/ --cov-report=term-missing tests "$@"
