#!/usr/bin/env bash
set -euo pipefail

./lint.sh
./test.sh "$@"
