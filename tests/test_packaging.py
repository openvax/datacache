"""Check the dependencies consumers receive from a normal installation."""

from importlib.metadata import metadata, requires

from packaging.requirements import Requirement


def test_pandas_three_is_an_unconditional_runtime_dependency():
    dependencies = [Requirement(value) for value in requires("datacache")]
    pandas_dependencies = [item for item in dependencies if item.name == "pandas"]
    assert len(pandas_dependencies) == 1
    dependency = pandas_dependencies[0]
    assert dependency.marker is None
    assert "2.3.3" not in dependency.specifier
    for version in ("3.0.0", "3.1.0", "3.99.0"):
        assert version in dependency.specifier
    assert "4.0.0" not in dependency.specifier


def test_tqdm_is_an_unconditional_runtime_dependency():
    dependencies = [Requirement(value) for value in requires("datacache")]
    tqdm_dependencies = [item for item in dependencies if item.name == "tqdm"]
    assert len(tqdm_dependencies) == 1
    dependency = tqdm_dependencies[0]
    assert dependency.marker is None
    assert "4.64.0" in dependency.specifier
    assert "4.63.0" not in dependency.specifier


def test_progress_extra_remains_a_compatible_alias():
    assert "progress" in metadata("datacache").get_all("Provides-Extra", [])


def test_python_support_metadata_has_no_future_upper_bound():
    from packaging.specifiers import SpecifierSet
    info = metadata('datacache')
    supported = SpecifierSet(info['Requires-Python'])
    assert '3.10' not in supported
    assert 'Programming Language :: Python :: 3.10' not in info.get_all('Classifier')
    for version in ('3.11', '3.12', '3.13', '3.14'):
        assert version in supported
        assert 'Programming Language :: Python :: ' + version in info.get_all('Classifier')
    assert '4.0' in supported
