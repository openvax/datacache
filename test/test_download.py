# Copyright (c) 2014. Mount Sinai School of Medicine
#
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

from datacache import fetch_file

FASTA_FILENAME = 'Homo_sapiens.GRCh37.75.dna_rm.chromosome.MT.fa'
URL = \
    'ftp://ftp.ensembl.org/pub/release-75/fasta/homo_sapiens/dna/Homo_sapiens.GRCh37.75.dna_rm.chromosome.MT.fa.gz'


def test_fetch_decompress():
    for use_wget_if_available in [True, False]:
        for timeout in [None, 10**6]:
            path1 = fetch_file(
                URL,
                decompress=True,
                subdir="datacache",
                use_wget_if_available=use_wget_if_available,
                timeout=timeout)
        assert path1.endswith(FASTA_FILENAME)
        with open(path1, 'r') as f1:
            s1 = f1.read()
            assert "TCAATTTCGTGCCAG" in s1

def test_fetch_subdirs():
    path = fetch_file(URL, decompress=True, subdir="datacache")
    assert path.endswith(FASTA_FILENAME)

    # if we change the subdir then data should end up in
    # something like /Users/me/Library/Caches/epitopes_test/
    other_path = fetch_file(URL, decompress=True, subdir="datacache_test")
    assert other_path.endswith(FASTA_FILENAME)
    assert other_path != path, other_path
