#!/bin/bash
set -o errexit

ruff check datacache/ tests/

echo 'Passes ruff check'
