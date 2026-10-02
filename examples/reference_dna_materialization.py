"""Tiny offline reference-DNA transaction; scientific work stays in the builder."""

import gzip
from hashlib import sha256
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

from datacache import inspect_materialization, materialize


def build_reference(source_paths, output_paths):
    with gzip.open(source_paths['dna.fa.gz'], 'rb') as source, open(output_paths['dna.fa'], 'wb') as output:
        shutil.copyfileobj(source, output, length=2 ** 20)
    # A toy caller-side biological check, not a DataCache FASTA parser.
    with open(output_paths['dna.fa'], 'rb') as fasta:
        if fasta.readline() != b'>chr1\n':
            raise ValueError('unexpected contig header')
        length = sum(len(line.strip()) for line in fasta)
    Path(output_paths['index.json']).write_text(json.dumps({'length': length}))


def main():
    with TemporaryDirectory() as temporary:
        root = Path(temporary)
        dna = b'>chr1\nACGTACGT\n'
        compressed = gzip.compress(dna, mtime=0)
        original = root / 'upstream.fa.gz'
        original.write_bytes(compressed)
        sources = {'dna.fa.gz': {'path': original, 'sha256': sha256(compressed).hexdigest(),
                                'size': len(compressed)}}
        outputs = {'dna.fa': {'sha256': sha256(dna).hexdigest(), 'size': len(dna)}, 'index.json': {}}
        transform = {'version': 'toy-fasta-1', 'options': {'contig': 'chr1'}}
        store = root / 'reference' / 'release-110'
        paths = materialize(store, sources, transform=transform, outputs=outputs, builder=build_reference,
                            download_options={'show_progress': False})
        assert Path(paths['dna.fa']).read_bytes() == dna
        assert json.loads(Path(paths['index.json']).read_text()) == {'length': 8}
        assert not list(store.glob('.inputs-*'))
        original.unlink()
        # Neither the upstream source nor a builder is needed on an offline hit.
        def forbidden(*args):
            raise AssertionError('offline reuse attempted to rebuild')
        assert materialize(store, sources, transform=transform, outputs=outputs,
                           builder=forbidden, verify_files=False) == paths
        state = inspect_materialization(store, sources, transform=transform, outputs=outputs)
        assert state.status == 'available'
        assert not state.verified  # Index has observed metadata, not a trusted hash.
        assert state.sources['dna.fa.gz']['verified']
        print('Reference DNA: atomic pair, compressed provenance, input cleanup and offline reuse pass')


if __name__ == '__main__':
    main()
