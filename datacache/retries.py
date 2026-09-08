"""Bounded retry policy for idempotent HTTP downloads."""

from datetime import timezone
from email.utils import parsedate_to_datetime
import math
from numbers import Real
import ssl
import time

import requests
from requests.packages.urllib3.exceptions import SSLError as Urllib3SSLError


DEFAULT_MAX_RETRIES = 2
DEFAULT_RETRY_BACKOFF = 1.0
DEFAULT_RETRY_MAX_DELAY = 30.0
RETRYABLE_HTTP_STATUSES = frozenset((408, 429, 500, 502, 503, 504))


def validate_retry_options(max_retries, retry_backoff, retry_max_delay):
    """Validate settings and return built-in float delays for arithmetic/sleep."""
    if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
        raise ValueError("max_retries must be a non-negative integer")
    delays = []
    for name, value in (("retry_backoff", retry_backoff), ("retry_max_delay", retry_max_delay)):
        message = "%s must be a finite non-negative number" % name
        if isinstance(value, bool) or not isinstance(value, Real) or value < 0:
            raise ValueError(message)
        try:
            delay = float(value)
        except (OverflowError, TypeError, ValueError) as error:
            raise ValueError(message) from error
        if not math.isfinite(delay):
            raise ValueError(message)
        delays.append(delay)
    return tuple(delays)


def _contains_tls_error(error):
    """Follow active causes and Requests/urllib3 wrappers, guarding cycles."""
    pending, seen = [error], set()
    while pending:
        current = pending.pop()
        if not isinstance(current, BaseException) or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, (ssl.SSLError, requests.exceptions.SSLError, Urllib3SSLError)):
            return True
        pending.extend(current.args)
        pending.extend((getattr(current, "reason", None), getattr(current, "original_error", None)))
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        elif not current.__suppress_context__:
            pending.append(current.__context__)
    return False


def is_retryable_http_error(error):
    if isinstance(error, requests.HTTPError):
        return error.response is not None and error.response.status_code in RETRYABLE_HTTP_STATUSES
    transient = isinstance(error, (
        requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError))
    return transient and not _contains_tls_error(error)


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
    return float(max(min(backoff, max_delay), retry_after))


def error_description(error):
    """Describe a failure without adding credentials or URL query text to logs."""
    if isinstance(error, requests.HTTPError) and error.response is not None:
        return "HTTP %s" % error.response.status_code
    return type(error).__name__
