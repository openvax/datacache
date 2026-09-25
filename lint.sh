#!/bin/bash
set -o errexit

ruff check datacache/ tests/ examples/

echo 'Passes ruff check'
