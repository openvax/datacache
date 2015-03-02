# Copyright (c) 2015. Mount Sinai School of Medicine
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

from Bio import SeqIO

from .download import fetch_file

def fetch_fasta_dict(download_url, filename=None, subdir=None):
    """
    Download a remote FASTA file from `download_url` and save it locally as `filename`.
    Load the file using BioPython and return an index mapping from entry keys to their sequence.
    """
    fasta_path = fetch_file(
        download_url=download_url,
        filename=filename,
        decompress=True,
        subdir=subdir)
    return SeqIO.index(fasta_path, 'fasta')
