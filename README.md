[![Tests](https://github.com/openvax/datacache/actions/workflows/tests.yml/badge.svg)](https://github.com/openvax/datacache/actions/workflows/tests.yml)
<a href="https://coveralls.io/github/openvax/datacache?branch=master">
<img src="https://coveralls.io/repos/openvax/datacache/badge.svg?branch=master&service=github" alt="Coverage Status" />
</a>
<a href="https://pypi.python.org/pypi/datacache/">
<img src="https://img.shields.io/pypi/v/datacache.svg?maxAge=1000" alt="PyPI" />
</a>

# DataCache

Helpers for transparently downloading datasets

## API

- **fetch_file**(download\_url, filename = _None_, decompress = _False_, subdir = _None_)
- **fetch_and_transform**(transformed\_filename, transformer, loader, source_filename, source_url, subdir = _None_)
- **fetch_fasta_dict**(download\_url, filename = _None_, subdir = _None_)
- **fetch_fasta_db**(table\_name, download_url, fasta_filename = \_None*, key\_column = _'id'_, value\_column = _'seq'_, subdir = _None_)
- **fetch_csv_db**(table\_name, download\_url, csv\_filename = _None_, subdir = _None_, \*\*pandas_kwargs)
