from Bio import SeqIO

from .download import fetch_file

def fetch_fasta_dict(download_url, filename = None, subdir = None):
    """
    Download a remote FASTA file from `download_url` and save it locally as `filename`.
    Load the file using BioPython and return an index mapping from entry keys to their sequence.
    """
    fasta_path = fetch_file(download_url = download_url, filename = filename, decompress = True, subdir = subdir)
    return SeqIO.index(fasta_path, 'fasta')
