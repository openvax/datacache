import gzip
import io
import logging
import subprocess
import sys

import pytest

from datacache.fasta import (
    LINE_WIDTH,
    FastaRecord,
    first_fasta_id,
    iter_fasta,
    iter_fasta_records,
    parse_fasta_header_id,
    read_fasta,
    write_fasta,
    write_fasta_records,
)


def test_fasta_module_is_stdlib_only():
    code = (
        "import json, sys, datacache.fasta\n"
        "heavy = {'requests', 'urllib3', 'pandas', 'numpy', 'sqlalchemy', 'filelock'}\n"
        "print(json.dumps(sorted(heavy & set(sys.modules))))\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )
    assert proc.stdout.strip() == "[]"


def test_iter_fasta_records_strict_and_upper():
    stream = io.StringIO(">seq1 first description\nacgt\nTGCA\n\n>seq2\nMKL*\n")
    records = list(iter_fasta_records(stream, upper=True))
    assert records == [
        FastaRecord("seq1 first description", "ACGTTGCA", 1),
        FastaRecord("seq2", "MKL*", 5),
    ]
    assert [r.identifier for r in records] == ["seq1", "seq2"]


def test_iter_fasta_auto_gzip_and_explicit_gzip(tmp_path):
    plain = tmp_path / "seqs.fa"
    plain.write_text(">p1 desc\nSIIN\nFEKL\n", encoding="utf-8")
    gz = tmp_path / "seqs.fa.gz"
    with gzip.open(gz, "wt", encoding="utf-8") as handle:
        handle.write(">p1 desc\nSIIN\nFEKL\n")
    disguised = tmp_path / "seqs.bin"
    disguised.write_bytes(gz.read_bytes())

    assert list(iter_fasta(plain)) == [("p1", "SIINFEKL")]
    assert list(iter_fasta(gz)) == [("p1", "SIINFEKL")]
    assert list(iter_fasta(disguised, gzipped=True)) == [("p1", "SIINFEKL")]


def test_iter_fasta_validation_modes():
    with pytest.raises(ValueError, match="FASTA header without an identifier"):
        list(iter_fasta_records(io.StringIO(">   \nACGT\n")))

    with pytest.raises(ValueError, match="FASTA sequence before the first header"):
        list(iter_fasta_records(io.StringIO("ACGT\n>a\nTTTT\n")))

    # Lenient comment + pre-header + empty-header modes (mhctools / pyensembl)
    stream = io.StringIO("; comment\nstray\n>a\nAC\n; mid comment\nGT\n>\nTT\n")
    records = list(
        iter_fasta_records(
            stream,
            ignore_comments=True,
            allow_pre_header=True,
            allow_empty_header=True,
        )
    )
    assert [(r.header, r.sequence) for r in records] == [("a", "ACGT"), ("", "TT")]
    assert records[1].identifier == ""


def test_iter_fasta_weirdo_strict_non_empty_modes():
    with pytest.raises(ValueError, match="no FASTA entries"):
        list(iter_fasta(io.StringIO("\n   \n"), require_non_empty=True))

    with pytest.raises(ValueError, match="has no sequence"):
        list(iter_fasta(io.StringIO(">p1\n>p2\nACGT\n"), allow_empty_sequence=False))

    with pytest.raises(ValueError, match="has no sequence"):
        list(iter_fasta(io.StringIO(">p1\n"), allow_empty_sequence=False))


def test_on_header_callback_and_split_pipe():
    calls = []
    stream = io.StringIO(
        ">ENSP00000493376.2|ENST00000641515.2|ENSG00000186092.7 extra\nMKL\n"
        ">ENSP00000123456.1 desc\nAAA\n"
    )
    result = list(iter_fasta(stream, split_pipe=True, on_header=lambda: calls.append(1)))
    assert result == [("ENSP00000493376.2", "MKL"), ("ENSP00000123456.1", "AAA")]
    assert len(calls) == 2


def test_parse_fasta_header_id():
    assert parse_fasta_header_id("ENSP1|ENST1 rest", split_pipe=True) == "ENSP1"
    assert parse_fasta_header_id("ENSP1|ENST1 rest", split_pipe=False) == "ENSP1|ENST1"
    assert parse_fasta_header_id(" leading_space") == ""
    with pytest.raises(ValueError, match="No identifier"):
        parse_fasta_header_id("")
    with pytest.raises(TypeError, match="Expected FASTA header to be str"):
        parse_fasta_header_id(b"ENSP1")


def test_read_fasta_duplicate_policies_and_skip_empty(caplog):
    text = ">a|x desc\nAAA\n>a|y desc\nCCC\n>empty\n> |no_id\nTTT\n"
    with pytest.raises(ValueError, match="FASTA identifier repeated: a"):
        read_fasta(io.StringIO(text), split_pipe=True)

    with caplog.at_level(logging.WARNING):
        last = read_fasta(
            io.StringIO(text),
            duplicate_policy="last",
            skip_empty_sequence=True,
            split_pipe=True,
        )
    assert last == {"a": "CCC"}
    assert "No sequence data for 'empty'" in caplog.text
    assert "Unable to parse ID" in caplog.text

    first = read_fasta(
        io.StringIO(">a\nAAA\n>a\nCCC\n"),
        duplicate_policy="first",
    )
    assert first == {"a": "AAA"}

    with pytest.raises(ValueError, match="duplicate_policy"):
        read_fasta(io.StringIO(">a\nAAA\n"), duplicate_policy="bogus")

    with pytest.raises(ValueError, match="No identifier on FASTA line"):
        read_fasta(io.StringIO(">\nAAA\n"), allow_empty_header=True)


def test_first_fasta_id(tmp_path):
    gz = tmp_path / "test.fa.gz"
    with gzip.open(gz, "wt", encoding="utf-8") as handle:
        handle.write("; comment\n>ENSP0001.2|ENST0001.2 gene\nMKL\n>ENSP0002.1\nAAA\n")
    assert first_fasta_id(gz, split_pipe=True) == "ENSP0001.2"
    assert first_fasta_id(gz, split_pipe=False) == "ENSP0001.2|ENST0001.2"
    assert first_fasta_id(io.StringIO("\n")) is None


def test_write_fasta_and_write_fasta_records(tmp_path):
    buf = io.StringIO()
    seq = "A" * (LINE_WIDTH + 5)
    write_fasta(buf, "id1", seq, description="human protein")
    lines = buf.getvalue().splitlines()
    assert lines == [">id1 human protein", "A" * LINE_WIDTH, "AAAAA"]

    buf_unwrapped = io.StringIO()
    write_fasta(buf_unwrapped, "id2", seq, line_width=None)
    assert buf_unwrapped.getvalue() == f">id2\n{seq}\n"

    with pytest.raises(ValueError, match="line_width"):
        write_fasta(io.StringIO(), "id", "AAA", line_width=0)

    out_path = tmp_path / "out.fa"
    write_fasta_records(out_path, [("h1\n", "ACGT"), ("h2", "TTTT")])
    assert out_path.read_text(encoding="utf-8") == ">h1\nACGT\n>h2\nTTTT\n"

    wrapped_buf = io.StringIO()
    write_fasta_records(wrapped_buf, [("h1", "ACGTAC")], line_width=4)
    assert wrapped_buf.getvalue() == ">h1\nACGT\nAC\n"

    with pytest.raises(ValueError, match="line_width"):
        write_fasta_records(io.StringIO(), [("h1", "ACGT")], line_width=0)
    with pytest.raises(TypeError, match="headers must be strings"):
        write_fasta_records(io.StringIO(), [(123, "ACGT")])
    with pytest.raises(TypeError, match="sequences must be strings"):
        write_fasta_records(io.StringIO(), [("h1", 123)])
