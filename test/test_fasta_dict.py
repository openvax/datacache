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
import gzip

URL = "".join([
    'ftp://ftp.ensembl.org/pub/release-75',
    '/fasta/homo_sapiens/dna/Homo_sapiens.GRCh37.75',
    '.dna_rm.chromosome.MT.fa.gz',
])

def fetch_fasta_dict(path_or_url):
    path = fetch_file(path_or_url)
    d = {}
    value_buffer = []
    key = None
    if path.endswith(".gz") or path.endswith(".gzip"):
        f = gzip.open(path, "r")
    else:
        f = open(path, "r")
    for line in f.readlines():
        if type(line) is bytes:
            line = line.decode("ascii")
        if line.startswith(">"):
            if key is not None:
                d[key] = "".join(value_buffer)
                value_buffer = []
            key = line.split()[0][1:]
        else:
            value_buffer.append(line.strip())
    if key and value_buffer:
        d[key] = "".join(value_buffer)
    f.close()
    return d


def test_download_fasta_dict():
    d = fetch_fasta_dict(URL)
    assert len(d) > 0
