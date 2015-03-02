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

from datacache import fetch_fasta_dict

FASTA_FILENAME = 'Homo_sapiens.GRCh37.75.dna_rm.chromosome.MT.fa'
URL = \
    'ftp://ftp.ensembl.org/pub/release-75/fasta/homo_sapiens/dna/Homo_sapiens.GRCh37.75.dna_rm.chromosome.MT.fa.gz'

def test_download_fasta_dict():
    d = fetch_fasta_dict(URL)
    assert hasattr(d, 'keys'), d
    assert hasattr(d, 'values'), d
    assert len(d) > 0

    d2 = fetch_fasta_dict(URL, filename=FASTA_FILENAME)
    assert len(d) == len(d2)