# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Lightweight standard-library FASTA readers and writers.

Shared across OpenVax / PIRL packages so plain and gzipped FASTA parsing,
header identifier extraction, validation, and writing live in one place with
zero third-party dependencies.
"""

from contextlib import nullcontext
from dataclasses import dataclass
import gzip
import logging
import os

logger = logging.getLogger(__name__)

LINE_WIDTH = 80


@dataclass(frozen=True)
class FastaRecord:
    """One FASTA record parsed from a file or text stream.

    Attributes
    ----------
    header : str
        Header line after the leading ``>``, with trailing line endings removed.
    sequence : str
        Concatenated sequence lines with surrounding whitespace stripped.
    line_number : int
        1-based line number of the ``>`` header line in the source.
    """

    header: str
    sequence: str
    line_number: int

    @property
    def identifier(self) -> str:
        """Return the first whitespace-delimited token of :attr:`header`."""
        parts = self.header.split(None, 1)
        return parts[0] if parts else ""


def _is_gzip_path(path):
    lower = os.fspath(path).lower()
    return lower.endswith((".gz", ".gzip"))


def _open_text_read(source, *, gzipped=None, encoding="utf-8"):
    if isinstance(source, (str, os.PathLike)):
        use_gzip = _is_gzip_path(source) if gzipped is None else bool(gzipped)
        opener = gzip.open if use_gzip else open
        return opener(source, "rt", encoding=encoding)
    return nullcontext(source)


def _open_text_write(target, *, encoding="utf-8"):
    if isinstance(target, (str, os.PathLike)):
        return open(os.fspath(target), "w", encoding=encoding)
    return nullcontext(target)


def _source_label(source, source_name):
    if source_name is not None:
        return source_name
    if isinstance(source, (str, os.PathLike)):
        return str(source)
    return getattr(source, "name", "<stream>")


def parse_fasta_header_id(header, *, split_pipe=False):
    """Extract the leading identifier from a FASTA header (text after ``>``).

    Parameters
    ----------
    header : str
        Header text after the leading ``>``.
    split_pipe : bool, optional
        When True, split the leading token at the first ``|`` (e.g. GENCODE
        headers like ``ENSP00000493376.2|ENST00000641515.2|...``).

    Returns
    -------
    str
        Leading identifier token, or ``""`` when ``header`` starts with
        whitespace or ``|`` after stripping a leading space.

    Raises
    ------
    ValueError
        If ``header`` is empty.
    """
    if not isinstance(header, str):
        raise TypeError(
            "Expected FASTA header to be str, got %s" % (type(header),)
        )
    if not header:
        raise ValueError("No identifier on FASTA line")
    identifier = header.split(" ", 1)[0]
    if split_pipe and "|" in identifier:
        identifier = identifier.split("|", 1)[0]
    return identifier


def iter_fasta_records(
    source,
    *,
    upper=False,
    gzipped=None,
    source_name=None,
    encoding="utf-8",
    ignore_comments=False,
    allow_empty_header=False,
    allow_pre_header=False,
    allow_empty_sequence=True,
    require_non_empty=False,
    on_header=None,
):
    """Yield :class:`FastaRecord` entries from a FASTA path or open text stream.

    Parameters
    ----------
    source : str, os.PathLike, or iterable of str
        Path to a plain or gzipped FASTA file, or an already-open text stream
        or line iterable.
    upper : bool, optional
        Upper-case each sequence line as it is read.
    gzipped : bool or None, optional
        When ``source`` is a path: ``True`` forces :func:`gzip.open`, ``False``
        forces plain :func:`open`, and ``None`` (default) auto-detects
        ``.gz``/``.gzip`` suffixes case-insensitively.
    source_name : str, optional
        Display label used in error messages.
    encoding : str, optional
        Text encoding used when opening a file path (default ``"utf-8"``).
    ignore_comments : bool, optional
        When True, ignore lines whose first non-whitespace character is ``;``.
    allow_empty_header : bool, optional
        When False (default), a ``>`` header with no non-whitespace identifier
        raises :class:`ValueError`. When True, such records are yielded with
        their raw header text.
    allow_pre_header : bool, optional
        When False (default), non-blank sequence text before the first ``>``
        raises :class:`ValueError`. When True, lines before the first header
        are ignored.
    allow_empty_sequence : bool, optional
        When True (default), records with no sequence lines are yielded with
        ``sequence=""``. When False, empty-sequence records raise
        :class:`ValueError`.
    require_non_empty : bool, optional
        When True, raise :class:`ValueError` if the input contains zero FASTA
        records.
    on_header : callable, optional
        Zero-argument callback invoked each time a ``>`` header line is read
        (for example, to update a progress bar).

    Yields
    ------
    FastaRecord
        Parsed record containing ``header``, ``sequence``, and 1-based
        ``line_number``.
    """
    label = _source_label(source, source_name)
    if isinstance(source, (str, os.PathLike)):
        with _open_text_read(source, gzipped=gzipped, encoding=encoding) as stream:
            yield from iter_fasta_records(
                stream,
                upper=upper,
                source_name=label,
                ignore_comments=ignore_comments,
                allow_empty_header=allow_empty_header,
                allow_pre_header=allow_pre_header,
                allow_empty_sequence=allow_empty_sequence,
                require_non_empty=require_non_empty,
                on_header=on_header,
            )
        return

    header = None
    parts = []
    start_line = None
    saw_record = False

    for number, line in enumerate(source, 1):
        text = line.rstrip("\r\n")
        stripped = text.strip()
        if not stripped:
            continue
        if ignore_comments and stripped.startswith(";"):
            continue
        if text.startswith(">") or stripped.startswith(">"):
            if on_header is not None:
                on_header()
            if header is not None:
                if not allow_empty_sequence and not parts:
                    ident = header.split(None, 1)[0] if header.strip() else ""
                    raise ValueError(
                        f"{label}: FASTA entry {ident!r} has no sequence"
                    )
                yield FastaRecord(header, "".join(parts), start_line)
            raw_header = text[1:] if text.startswith(">") else stripped[1:]
            if not allow_empty_header and not raw_header.strip():
                raise ValueError(
                    f"FASTA header without an identifier: {label}:{number}"
                )
            header = raw_header
            parts = []
            start_line = number
            saw_record = True
        elif header is not None:
            parts.append(stripped.upper() if upper else stripped)
        elif not allow_pre_header:
            raise ValueError(
                f"FASTA sequence before the first header: {label}:{number}"
            )

    if header is not None:
        if not allow_empty_sequence and not parts:
            ident = header.split(None, 1)[0] if header.strip() else ""
            raise ValueError(
                f"{label}: FASTA entry {ident!r} has no sequence"
            )
        yield FastaRecord(header, "".join(parts), start_line)
    elif require_non_empty and not saw_record:
        raise ValueError(f"{label}: no FASTA entries")


def iter_fasta(
    source,
    *,
    upper=False,
    gzipped=None,
    source_name=None,
    encoding="utf-8",
    ignore_comments=False,
    allow_empty_header=False,
    allow_pre_header=False,
    allow_empty_sequence=True,
    require_non_empty=False,
    split_pipe=False,
    on_header=None,
):
    """Yield ``(identifier, sequence)`` pairs for every record in a FASTA source.

    Accepts the same keyword options as :func:`iter_fasta_records`, plus
    ``split_pipe`` for GENCODE pipe-delimited headers.

    Parameters
    ----------
    source : str, os.PathLike, or iterable of str
        Path to a plain or gzipped FASTA file, or an already-open text stream.
    upper : bool, optional
        Upper-case each sequence line as it is read.
    gzipped : bool or None, optional
        Force gzipped/plain opening, or auto-detect ``.gz``/``.gzip`` when
        ``None``.
    source_name : str, optional
        Display label used in error messages.
    encoding : str, optional
        Text encoding used when opening a file path (default ``"utf-8"``).
    ignore_comments : bool, optional
        Ignore ``;``-prefixed comment lines when True.
    allow_empty_header : bool, optional
        Allow ``>`` headers without a non-whitespace identifier when True.
    allow_pre_header : bool, optional
        Ignore non-blank lines before the first ``>`` header when True.
    allow_empty_sequence : bool, optional
        Allow records with empty sequences when True; raise when False.
    require_non_empty : bool, optional
        Raise :class:`ValueError` when the source has zero FASTA entries.
    split_pipe : bool, optional
        When True, split the first token at ``|`` (for GENCODE pipe-delimited
        headers).
    on_header : callable, optional
        Zero-argument callback invoked on each ``>`` header line.

    Yields
    ------
    tuple of (str, str)
        ``(identifier, sequence)`` pairs in file order.
    """
    for record in iter_fasta_records(
        source,
        upper=upper,
        gzipped=gzipped,
        source_name=source_name,
        encoding=encoding,
        ignore_comments=ignore_comments,
        allow_empty_header=allow_empty_header,
        allow_pre_header=allow_pre_header,
        allow_empty_sequence=allow_empty_sequence,
        require_non_empty=require_non_empty,
        on_header=on_header,
    ):
        identifier = record.identifier
        if split_pipe and "|" in identifier:
            identifier = identifier.split("|", 1)[0]
        yield identifier, record.sequence


def read_fasta(
    source,
    *,
    upper=False,
    gzipped=None,
    source_name=None,
    encoding="utf-8",
    duplicate_policy="error",
    skip_empty_sequence=False,
    ignore_comments=False,
    allow_empty_header=False,
    allow_pre_header=False,
    split_pipe=False,
):
    """Read a FASTA file or stream into ``{identifier: sequence}`` in file order.

    Parameters
    ----------
    source : str, os.PathLike, or iterable of str
        Path to a plain or gzipped FASTA file, or an already-open text stream.
    upper : bool, optional
        Upper-case each sequence line as it is read.
    gzipped : bool or None, optional
        Force gzipped/plain opening, or auto-detect ``.gz``/``.gzip`` when
        ``None``.
    source_name : str, optional
        Display label used in error messages.
    encoding : str, optional
        Text encoding used when opening a file path (default ``"utf-8"``).
    duplicate_policy : {"error", "last", "first"}, optional
        How repeated identifiers are handled: ``"error"`` (default) raises
        :class:`ValueError`, ``"last"`` keeps the last occurrence, and
        ``"first"`` keeps the first occurrence.
    skip_empty_sequence : bool, optional
        When True, log a warning and omit records with an empty sequence
        (or empty identifier when ``allow_empty_header=True``).
    ignore_comments : bool, optional
        Ignore ``;``-prefixed comment lines when True.
    allow_empty_header : bool, optional
        Allow ``>`` headers without a non-whitespace identifier when True.
    allow_pre_header : bool, optional
        Ignore non-blank lines before the first ``>`` header when True.
    split_pipe : bool, optional
        When True, split the first token at ``|`` (for GENCODE pipe-delimited
        headers).

    Returns
    -------
    dict of str to str
        Mapping from record identifier to sequence.
    """
    if duplicate_policy not in ("error", "last", "first"):
        raise ValueError(
            "duplicate_policy must be 'error', 'last', or 'first', got %r"
            % (duplicate_policy,)
        )
    label = _source_label(source, source_name)
    records = {}
    for record in iter_fasta_records(
        source,
        upper=upper,
        gzipped=gzipped,
        source_name=label,
        encoding=encoding,
        ignore_comments=ignore_comments,
        allow_empty_header=allow_empty_header,
        allow_pre_header=allow_pre_header,
    ):
        if allow_empty_header and not record.header:
            raise ValueError("No identifier on FASTA line")
        identifier = (
            parse_fasta_header_id(record.header, split_pipe=split_pipe)
            if split_pipe
            else record.identifier
        )
        if not identifier:
            logger.warning("Unable to parse ID from header line: >%s", record.header)
            continue
        if skip_empty_sequence and not record.sequence:
            logger.warning("No sequence data for '%s'", identifier)
            continue
        if identifier in records:
            if duplicate_policy == "error":
                raise ValueError(
                    f"FASTA identifier repeated: {identifier} in {label}"
                )
            if duplicate_policy == "first":
                continue
        records[identifier] = record.sequence
    return records


def first_fasta_id(source, *, gzipped=None, split_pipe=False, encoding="utf-8"):
    """Return the identifier of the first FASTA record, or ``None`` if empty."""
    with _open_text_read(source, gzipped=gzipped, encoding=encoding) as stream:
        for line in stream:
            stripped = line.rstrip("\r\n")
            if stripped.startswith(">"):
                return parse_fasta_header_id(stripped[1:], split_pipe=split_pipe)
    return None


def write_fasta(stream, identifier, sequence, *, description="", line_width=LINE_WIDTH):
    """Write one FASTA record to a writable text stream.

    Parameters
    ----------
    stream : io.TextIOBase
        Writable text stream.
    identifier : str
        Record identifier, written as the first header token.
    sequence : str
        Nucleotide or amino-acid sequence.
    description : str, optional
        Optional header description written after a space following
        ``identifier``.
    line_width : int or None, optional
        Wrap sequence lines to this width (default :data:`LINE_WIDTH`, 80).
        Pass ``None`` to write the entire sequence on a single line.
    """
    header = f"{identifier} {description}".rstrip()
    stream.write(f">{header}\n")
    if line_width is None:
        stream.write(f"{sequence}\n")
        return
    if line_width <= 0:
        raise ValueError(f"line_width must be positive or None, got {line_width!r}")
    for offset in range(0, len(sequence), line_width):
        stream.write(f"{sequence[offset:offset + line_width]}\n")


def write_fasta_records(target, records, *, line_width=None, encoding="utf-8"):
    """Write ``(header, sequence)`` pairs to a FASTA path or writable stream."""
    with _open_text_write(target, encoding=encoding) as handle:
        for header, sequence in records:
            if not isinstance(header, str):
                raise TypeError("FASTA headers must be strings")
            if not isinstance(sequence, str):
                raise TypeError("FASTA sequences must be strings")
            clean_header = header.replace("\n", "").strip()
            handle.write(f">{clean_header}\n")
            if line_width is None:
                handle.write(f"{sequence}\n")
            else:
                if line_width <= 0:
                    raise ValueError(
                        f"line_width must be positive or None, got {line_width!r}"
                    )
                for offset in range(0, len(sequence), line_width):
                    handle.write(f"{sequence[offset:offset + line_width]}\n")


__all__ = [
    "LINE_WIDTH",
    "FastaRecord",
    "first_fasta_id",
    "iter_fasta",
    "iter_fasta_records",
    "parse_fasta_header_id",
    "read_fasta",
    "write_fasta",
    "write_fasta_records",
]
