"""Run with python -m examples.versioned_datasets; no network required."""

from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

from datacache import VersionedDatasetRegistry, db_from_dataframe, inspect_bundle


def main():
    import pandas as pd
    with TemporaryDirectory() as temporary:
        root = Path(temporary)
        upstream = root / 'upstream'
        upstream.mkdir()
        assets = {}
        for name, content in [('records.csv', b'id,sequence\n1,ACGT\n'),
                              ('manifest.json', b'{"release":"2026-09"}\n'),
                              ('source.txt', b'example source\n')]:
            source = upstream / name
            source.write_bytes(content)
            assets[name] = dict(url=source.as_uri(), size=len(content),
                                sha256=sha256(content).hexdigest())
        datasets = {
            name: dict(default_version='2026-09', versions={'2026-09': selected})
            for name, selected in [
                ('single', {'records.csv': assets['records.csv']}),
                ('pair', {k: assets[k] for k in ('records.csv', 'manifest.json')}),
                ('multi', assets),
            ]
        }
        registry = VersionedDatasetRegistry(datasets, cache_root=root / 'shared')
        assert registry.inspect('single').status == 'missing'
        assert not (root / 'shared').exists()
        for name in datasets:
            paths = registry.download(name)
            assert set(paths) == set(datasets[name]['versions']['2026-09'])
            assert registry.inspect(name).verified
        generated = root / 'generated'
        generated.mkdir()
        table = pd.read_csv(registry.local_path('pair', asset='records.csv'))
        connection = db_from_dataframe(str(generated / 'sequences.db'), 'sequences', table,
                                       primary_key='id')
        try:
            assert connection.execute('SELECT sequence FROM sequences WHERE id=1').fetchone() == ('ACGT',)
        finally:
            connection.close()
        registry.download('pair', force=True)
        assert (generated / 'sequences.db').is_file()
        for source in upstream.iterdir():
            source.unlink()
        consumer = VersionedDatasetRegistry(datasets, cache_root=root / 'shared')
        for name in datasets:
            consumer.download(name)  # Verified reuse with no upstream available.
            assert consumer.inspect(name).verified
            assert inspect_bundle(consumer.store_path(name)).status == 'available'
        print('Three dataset shapes installed, indexed, shared, and inspected offline.')


if __name__ == '__main__':
    main()
