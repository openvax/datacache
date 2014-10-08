datacache
=========

Helpers for transparently downloading datasets

## API

* **fetch_file**(download_url, filename = *None*, decompress = *False*, subdir = *None*)
* **fetch_and_transform**(transformed_filename, transformer, loader,
        source_filename, source_url, subdir = *None*)
* **fetch_fasta_dict**(download_url, filename = *None*, subdir = *None*)
* **fetch_fasta_db**(table_name, download_url, fasta_filename = *None*,
        key_column = *'id'*, value_column = *'seq'*, subdir = *None*)
* **fetch_csv_db**(table_name, download_url, csv_filename = *None*, subdir = *None*,
        \*\*pandas_kwargs)

