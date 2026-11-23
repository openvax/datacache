# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Global offline-mode kill-switch for datacache and OpenVax consumers."""

from os import environ
from urllib.parse import urlsplit

OFFLINE_ENV_KEYS = ("DATACACHE_OFFLINE", "OPENVAX_OFFLINE")
_REMOTE_SCHEMES = frozenset({"http", "https", "ftp", "ftps"})
_TRUTHY = frozenset({"1", "true", "yes", "on"})
_FALSY = frozenset({"0", "false", "no", "off", ""})


class OfflineError(OSError):
    """Raised when a remote network transfer is attempted while offline mode is active.

    Subclasses ``OSError`` so callers that already catch transport/IO failures
    or enable ``return_stale_on_error`` handle offline refusals cleanly while
    allowing callers to distinguish explicit offline blocks from network errors.
    """


def _parse_bool_env(key, raw):
    normalized = raw.strip().lower()
    if normalized in _TRUTHY:
        return True
    if normalized in _FALSY:
        return False
    raise ValueError(
        "%s must be a boolean value (1/0, true/false, yes/no, on/off), got %r"
        % (key, raw)
    )


def offline_source(offline=None):
    """Return why offline mode is active (e.g. ``'offline=True'`` or ``'OPENVAX_OFFLINE=1'``), or None."""
    if offline is not None:
        if not isinstance(offline, bool):
            raise ValueError("offline must be a boolean or None, not %s" % type(offline).__name__)
        return "offline=True" if offline else None
    for key in OFFLINE_ENV_KEYS:
        raw = environ.get(key)
        if raw is not None and raw.strip():
            if _parse_bool_env(key, raw):
                return "%s=%s" % (key, raw.strip())
            return None
    return None


def is_offline(offline=None):
    """Return True when remote downloads are forbidden by ``offline`` or environment variables.

    Checks explicit ``offline`` first, then ``DATACACHE_OFFLINE``, then
    ``OPENVAX_OFFLINE``. An explicit ``offline=False`` overrides environment
    variables; an unset or false ``DATACACHE_OFFLINE`` allows ``OPENVAX_OFFLINE``
    only when ``DATACACHE_OFFLINE`` is unset/empty.
    """
    return offline_source(offline) is not None


def is_remote_url(url):
    """Return True if ``url`` uses a remote network scheme (HTTP, HTTPS, FTP, FTPS)."""
    if not isinstance(url, str):
        return False
    return urlsplit(url).scheme.lower() in _REMOTE_SCHEMES


def require_online(url, *, offline=None):
    """Raise :class:`OfflineError` if offline mode is active and ``url`` is remote.

    Local paths and ``file://`` URLs are always allowed offline.
    """
    if not is_remote_url(url):
        return
    reason = offline_source(offline)
    if reason is not None:
        raise OfflineError(
            "Offline mode is enabled (%s); refusing to download %s" % (reason, url)
        )
