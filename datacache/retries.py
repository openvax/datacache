"""Bounded retry policy for idempotent HTTP downloads."""

from datetime import timezone
from email.utils import parsedate_to_datetime
from numbers import Real
import sys
import time

import requests


DEFAULT_MAX_RETRIES = 2
DEFAULT_RETRY_BACKOFF = 1.0
DEFAULT_RETRY_MAX_DELAY = 30.0
RETRYABLE_HTTP_STATUSES = frozenset((408, 429, 500, 502, 503, 504))


def validate_retry_options(max_retries, retry_backoff, retry_max_delay):
    if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
        raise ValueError("max_retries must be a non-negative integer")
    for name, value in (("retry_backoff", retry_backoff), ("retry_max_delay", retry_max_delay)):
        if (isinstance(value, bool) or not isinstance(value, Real) or
                not (0 <= value <= sys.float_info.max)):
            raise ValueError("%s must be a finite non-negative number" % name)


def is_retryable_http_error(error):
    if isinstance(error, requests.exceptions.SSLError):
        return False
    if isinstance(error, requests.HTTPError):
        return error.response is not None and error.response.status_code in RETRYABLE_HTTP_STATUSES
    return isinstance(error, (
        requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError))


def retry_delay(error, backoff, max_delay):
    """Return a delay, or None if Retry-After exceeds the caller's wait limit.

    Invalid Retry-After values are ignored. A server's minimum wait is never
    shortened to fit the cap: in that case the original failure must propagate.
    """
    response = getattr(error, "response", None)
    header = response.headers.get("Retry-After") if response is not None else None
    retry_after = 0
    if header:
        header = header.strip()
        try:
            if header.isascii() and header.isdigit():
                digits = header.lstrip("0") or "0"
                # Avoid integer-conversion limits for an enormous server wait.
                if len(digits) > len(str(int(max_delay))):
                    return None
                retry_after = int(digits)
            else:
                date = parsedate_to_datetime(header)
                if date.tzinfo is None:
                    date = date.replace(tzinfo=timezone.utc)
                retry_after = max(0, date.timestamp() - time.time())
        except (TypeError, ValueError, OverflowError, OSError):
            pass
    if retry_after > max_delay:
        return None
    return max(min(backoff, max_delay), retry_after)


def error_description(error):
    """Describe a failure without adding credentials or URL query text to logs."""
    if isinstance(error, requests.HTTPError) and error.response is not None:
        return "HTTP %s" % error.response.status_code
    return type(error).__name__
