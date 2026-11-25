"""Tests for shared CLI help styling, completion, and datacache CLI flags."""

import io
import json
import subprocess
import sys

import pytest

from datacache import cli
from datacache.cli_help import (
    ANSI_BOLD,
    ANSI_BOLD_CYAN,
    ANSI_CYAN,
    ANSI_DIM,
    ANSI_RESET,
    ANSI_YELLOW,
    HelpArgumentParser,
    classify_status,
    color_enabled,
    enable_argcomplete,
    style_help,
    style_status,
    style_table_header,
)


class _Stream(io.StringIO):
    def __init__(self, tty):
        super().__init__()
        self._tty = tty

    def isatty(self):
        return self._tty


def test_color_enabled_respects_tty_no_color_and_dumb_term(monkeypatch):
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.delenv("TERM", raising=False)
    assert color_enabled(_Stream(True)) is True
    assert color_enabled(_Stream(False)) is False

    monkeypatch.setenv("NO_COLOR", "1")
    assert color_enabled(_Stream(True)) is False
    monkeypatch.delenv("NO_COLOR", raising=False)

    monkeypatch.setenv("TERM", "dumb")
    assert color_enabled(_Stream(True)) is False


def test_help_argument_parser_plain_format_and_tty_print(monkeypatch):
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.delenv("TERM", raising=False)

    parser = HelpArgumentParser(prog="demo", description="Demo CLI.")
    parser.add_argument("--cache-root", help="Cache directory ``path``.")

    plain = parser.format_help()
    assert "\033[" not in plain
    assert "``" not in plain

    tty_stream = _Stream(True)
    parser.print_help(file=tty_stream)
    styled = tty_stream.getvalue()
    assert ANSI_BOLD + "usage:" + ANSI_RESET in styled
    assert ANSI_CYAN + "--cache-root" + ANSI_RESET in styled
    assert ANSI_BOLD_CYAN in styled

    non_tty_stream = _Stream(False)
    parser.print_help(file=non_tty_stream)
    assert non_tty_stream.getvalue() == plain
    assert style_help(plain, non_tty_stream) == plain


def test_style_table_header_and_status(monkeypatch):
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.delenv("TERM", raising=False)

    table = "NAME  STATUS\nfoo   OK\n"
    assert style_table_header(table, use_color=False) == table
    styled = style_table_header(table, use_color=True)
    assert styled.startswith(ANSI_BOLD_CYAN + "NAME  STATUS" + ANSI_RESET + "\n")
    assert styled.endswith("foo   OK\n")

    assert classify_status("OK") == "plain"
    assert classify_status("available") == "plain"
    assert classify_status("absent") == "dim"
    assert classify_status("not installed") == "dim"
    assert classify_status("damaged") == "warning"
    assert classify_status("ok, stale") == "warning"
    assert classify_status("missing", dim_values=frozenset({"missing"})) == "dim"

    assert style_status("OK", use_color=True) == "OK"
    assert style_status("absent", use_color=True) == ANSI_DIM + "absent" + ANSI_RESET
    assert style_status("damaged", use_color=True) == ANSI_YELLOW + "damaged" + ANSI_RESET
    assert style_status("damaged", use_color=False) == "damaged"
    with pytest.raises(ValueError, match="Unknown status level"):
        style_status("x", level="bogus", use_color=True)


def test_enable_argcomplete_invokes_argcomplete(monkeypatch):
    called = []
    fake_mod = type("FakeArgcomplete", (), {"autocomplete": staticmethod(lambda p, **kw: called.append((p, kw)))})
    monkeypatch.setitem(sys.modules, "argcomplete", fake_mod)
    parser = HelpArgumentParser(prog="demo")
    assert enable_argcomplete(parser) is True
    assert called == [(parser, {})]


def test_cli_help_import_stays_stdlib_only():
    code = (
        "import sys; from datacache.cli_help import HelpArgumentParser; "
        "heavy = [m for m in ('pandas', 'requests', 'tqdm', 'filelock', 'sqlite3') if m in sys.modules]; "
        "assert not heavy, heavy"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_datacache_cli_accepts_cache_root_and_uses_help_parser(tmp_path, capsys):
    parser = cli.build_parser()
    assert isinstance(parser, HelpArgumentParser)

    root = tmp_path / "openvax"
    cli.run(["path", "--cache-root", str(root)])
    assert capsys.readouterr().out.strip() == str(root)

    cli.run(["dirs", "--cache-root", str(root), "--all", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert payload["shared_root"] == str(root)
    packages = {row["package"] for row in payload["caches"]}
    assert {"mhctools", "mhcgnomes"}.issubset(packages)

    cli.run(["scan", "--cache-root", str(root), "--json"])
    scan_payload = json.loads(capsys.readouterr().out)
    assert scan_payload["root"] == str(root)
