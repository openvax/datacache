"""The lazily imported package namespace offers everything the eager one did."""

import ast
import importlib
import json
from pathlib import Path
import pkgutil
import subprocess
import sys

import pytest

import datacache

PUBLIC_SUBMODULES = sorted(
    module.name for module in pkgutil.iter_modules(datacache.__path__) if not module.name.startswith("_"))


def run_in_fresh_interpreter(code):
    """Run ``code`` with ``python -c`` and decode the JSON its last line prints.

    Parameters
    ----------
    code : str
        Python source whose final output line is a JSON document.

    Returns
    -------
    object
        The decoded JSON value.
    """
    output = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout
    return json.loads(output.splitlines()[-1])


def test_every_public_name_has_a_defining_submodule():
    assert set(datacache.EXPORT_MODULES) | {"__version__"} == set(datacache.__all__)
    assert len(datacache.__all__) == len(set(datacache.__all__))
    for name, module_name in datacache.EXPORT_MODULES.items():
        module = importlib.import_module("datacache." + module_name)
        assert getattr(datacache, name) is getattr(module, name), name


def test_type_checking_imports_match_the_lazy_exports():
    # Type checkers and IDEs read the ``if TYPE_CHECKING:`` imports instead of
    # running ``__getattr__``; they must name the same objects.
    tree = ast.parse(Path(datacache.__file__).read_text())
    block = next(node for node in tree.body
                 if isinstance(node, ast.If) and getattr(node.test, "id", None) == "TYPE_CHECKING")
    imported = {alias.name: statement.module for statement in block.body for alias in statement.names}
    assert imported == datacache.EXPORT_MODULES


def test_every_name_in_all_resolves_in_a_fresh_interpreter():
    # In this process other tests may already have imported the submodules;
    # a fresh interpreter makes each lookup go through the lazy path.
    missing = run_in_fresh_interpreter(
        "import json, datacache\n"
        "print(json.dumps([name for name in datacache.__all__ if getattr(datacache, name, None) is None]))")
    assert missing == []
    star = run_in_fresh_interpreter(
        "import json\nfrom datacache import *\nprint(json.dumps(sorted(name for name in dir() if not name.startswith('_') or name == '__version__')))")
    assert set(datacache.__all__) <= set(star)


def test_dir_lists_public_names_before_they_are_imported():
    listed = run_in_fresh_interpreter("import json, datacache\nprint(json.dumps(dir(datacache)))")
    assert set(datacache.__all__) <= set(listed)
    assert listed == sorted(listed)
    assert set(datacache.__all__) <= set(dir(datacache))


@pytest.mark.parametrize("submodule", PUBLIC_SUBMODULES)
def test_submodules_are_attributes_after_a_bare_import(submodule):
    # Before 1.30 ``import datacache`` imported every submodule, so code such
    # as ``datacache.download.fetch_file`` worked without importing it.
    is_module = run_in_fresh_interpreter(
        "import json, sys, datacache\n"
        "module = datacache.%s\n"
        "print(json.dumps(module is sys.modules['datacache.%s']))" % (submodule, submodule))
    assert is_module is True


def test_downstream_submodule_imports_keep_working():
    # orfdata: ``from datacache import bundle_store``; pyensembl tests:
    # ``datacache.download.time`` after ``import datacache.download``.
    found = run_in_fresh_interpreter(
        "import json, datacache\n"
        "from datacache import bundle_store\n"
        "import datacache.download\n"
        "print(json.dumps([hasattr(bundle_store, 'BundleStore'), hasattr(datacache.download, 'time'),\n"
        "                  datacache.bundles.install_bundle is datacache.install_bundle]))")
    assert found == [True, True, True]


@pytest.mark.parametrize("name", ["not_a_datacache_name", "_filesystem_typo", "__wrapped__"])
def test_unknown_names_raise_attribute_error(name):
    with pytest.raises(AttributeError, match="has no attribute %r" % name):
        getattr(datacache, name)
    assert not hasattr(datacache, name)


def test_monkeypatching_a_public_name_is_undone(monkeypatch):
    original = datacache.fetch_file
    with monkeypatch.context() as patch:
        patch.setattr(datacache, "fetch_file", "replacement")
        assert datacache.fetch_file == "replacement"
    assert datacache.fetch_file is original
    with monkeypatch.context() as patch:
        patch.setattr("datacache.get_cache_root", "replacement")
        assert datacache.get_cache_root == "replacement"
    assert datacache.get_cache_root is datacache.common.get_cache_root
