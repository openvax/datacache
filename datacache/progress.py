"""Opt-in progress displays using the required, lazily imported tqdm dependency."""

from os import environ
import sys

NO_PROGRESS_ENV_KEYS = ("DATACACHE_NO_PROGRESS", "OPENVAX_NO_PROGRESS")
"""Environment variable names checked in order by ``should_show_progress``."""
_TRUTHY = frozenset({"1", "true", "yes", "on"})
_FALSY = frozenset({"0", "false", "no", "off", ""})


def _progress_disabled_by_env():
    for key in NO_PROGRESS_ENV_KEYS:
        raw = environ.get(key)
        if raw is not None and raw.strip():
            normalized = raw.strip().lower()
            if normalized in _TRUTHY:
                return True
            if normalized in _FALSY:
                return False
            raise ValueError(
                "%s must be a boolean value (1/0, true/false, yes/no, on/off), got %r"
                % (key, raw)
            )
    return False


def should_show_progress(show_progress=None, *, stream=None):
    """Decide whether a progress bar should render on ``stream`` (default ``sys.stderr``).

    Parameters
    ----------
    show_progress : bool, ``"auto"``, or None, optional
        - ``False``: always disabled.
        - ``True``: enabled unless disabled by ``DATACACHE_NO_PROGRESS`` or
          ``OPENVAX_NO_PROGRESS``.
        - ``None`` or ``"auto"``: enabled when not disabled by environment
          variables and ``stream`` is an interactive TTY (or running in an
          already-imported IPython kernel).
    stream : file-like or None, optional
        Output stream to inspect for TTY capability; defaults to ``sys.stderr``.

    Returns
    -------
    bool
        ``True`` if progress bars should render, ``False`` otherwise.

    Raises
    ------
    ValueError
        If ``show_progress`` is invalid or a ``NO_PROGRESS_ENV_KEYS`` variable
        holds an unrecognized boolean value.
    """
    if show_progress is False:
        return False
    if _progress_disabled_by_env():
        return False
    if show_progress is True:
        return True
    if show_progress not in (None, "auto"):
        raise ValueError("show_progress must be bool, 'auto', or None, got %r" % (show_progress,))
    target = sys.stderr if stream is None else stream
    if getattr(target, "isatty", lambda: False)():
        return True
    ipython = sys.modules.get("IPython")
    shell = ipython.get_ipython() if ipython is not None else None
    return shell is not None and getattr(shell, "kernel", None) is not None


class Progress:
    """Own one tqdm bar (on stderr) for one operation or download attempt."""

    def __init__(self, enabled=False, description="", total=None, unit="B"):
        self.enabled = should_show_progress(enabled)
        self.description = description
        self.total = total
        self.unit = unit
        self.bar = None

    def __enter__(self):
        if self.enabled:
            try:
                from tqdm.auto import tqdm
            except ImportError as error:
                raise ImportError(
                    "tqdm is a required datacache dependency; install 'tqdm>=4.64' "
                    "or reinstall datacache") from error
            self.bar = tqdm(
                desc=self.description, total=self.total, unit=self.unit,
                unit_scale=self.unit == "B", unit_divisor=1024,
                dynamic_ncols=True, leave=False, file=sys.stderr)
        return self

    def __call__(self, completed, total):
        if self.bar is not None:
            if completed < self.bar.n:
                self.bar.reset(total=total)
            self.bar.total = total
            self.bar.update(completed - self.bar.n)

    def __exit__(self, *exc):
        if self.bar is not None:
            self.bar.close()
