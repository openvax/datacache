"""Caller assertions of source identity, separate from content verification."""

import json

MAX_IDENTITY_BYTES = 64 * 1024
MAX_IDENTITY_DEPTH = 64


def _validate_json(value, depth=0):
    if depth > MAX_IDENTITY_DEPTH:
        raise ValueError('source_identity exceeds 64 levels of nesting')
    if type(value) is dict:
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError('source_identity object keys must be strings')
            _validate_json(item, depth + 1)
    elif type(value) is list:
        for item in value:
            _validate_json(item, depth + 1)
    elif type(value) not in (str, int, float, bool, type(None)):
        raise ValueError('source_identity must be a JSON value')


def _canonical(value):
    _validate_json(value)
    encoded = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
    # ensure_ascii=True makes this both a character count and a byte count.
    if len(encoded) > MAX_IDENTITY_BYTES:
        raise ValueError('source_identity exceeds 64 KiB of canonical JSON')
    return encoded


def normalize_source_identity(value):
    """Validate and detach a bounded JSON value from the caller's objects."""
    return json.loads(_canonical(value))


def source_identity_matches(recorded, requested):
    """Compare declarations, distinguishing absence, null, booleans and numbers."""
    if 'source_identity' not in recorded or 'source_identity' not in requested:
        return ('source_identity' in recorded) == ('source_identity' in requested)
    return _canonical(recorded['source_identity']) == _canonical(requested['source_identity'])
