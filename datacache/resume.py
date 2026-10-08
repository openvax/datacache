"""Hash- or strong-ETag-validated HTTP downloads with private partials."""

from contextlib import closing
import hashlib
import logging
import os
from pathlib import Path
import re
import stat
import time
from urllib.parse import urlsplit

from ._filesystem import file_lock, open_regular, path_present, read_json, write_json
from .common import USER_AGENT
from .integrity import FileValidationError, _validate_expectations, validate_file
from .progress import Progress
from .retries import error_description, is_retryable_http_error, retry_delay

logger = logging.getLogger(__name__)


def validate_resume(download_url, expected_sha256, expected_size):
    _validate_expectations(expected_sha256, expected_size)
    if expected_size is None:
        raise ValueError('resume=True requires expected_size')
    if urlsplit(download_url).scheme.lower() not in ('http', 'https'):
        raise ValueError('resume=True supports only raw HTTP/HTTPS downloads')
    if os.name != 'posix':
        raise NotImplementedError('Resumable downloads require a POSIX local filesystem')


def _state_directory(destination):
    destination = Path(destination)
    key = hashlib.sha256(os.fsencode(destination.name)).hexdigest()[:32]
    return destination.parent / ('.datacache-resume-%d-%s' % (os.getuid(), key))


def _prepare_directory(destination):
    directory = _state_directory(destination)
    directory.mkdir(mode=0o700, exist_ok=True)
    info = directory.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) & 0o077):
        raise FileValidationError(directory, 'resume directory must be owner-only and not a link')
    return directory


def discard_partial(destination):
    """Discard this user's resumable bytes for an exact destination, under lock.

    Does not change the installed file. Missing state is a no-op. The small
    private directory and permanent lock remain to coordinate concurrent calls.
    """
    if os.name != 'posix':
        raise NotImplementedError('Resumable downloads require a POSIX local filesystem')
    directory = _state_directory(destination)
    if not path_present(directory):
        return
    directory = _prepare_directory(destination)
    with file_lock(directory / 'lock', private=True):
        for name in ('partial', 'metadata.json'):
            (directory / name).unlink(missing_ok=True)


def _valid_validator(validator):
    """Whether a recorded transport validator is a usable If-Range value:
    {header: ETag (strong) or Last-Modified, value: one header line}."""
    return (
        isinstance(validator, dict) and set(validator) == {'header', 'value'}
        and validator['header'] in ('ETag', 'Last-Modified')
        and isinstance(validator['value'], str)
        and '\r' not in validator['value'] and '\n' not in validator['value']
        and (validator['header'] != 'ETag' or _strong_etag(validator['value'])))


def _strong_etag(value):
    # RFC 9110 entity-tag syntax, without the weak W/ prefix. Embedded quotes,
    # whitespace, and control characters cannot authorize an If-Range request.
    return isinstance(value, str) and re.fullmatch(r'"[\x21\x23-\x7e\x80-\xff]*"', value) is not None


def _validator(headers):
    etag = headers.get('ETag', '')
    if _strong_etag(etag):
        return {'header': 'ETag', 'value': etag}
    # Last-Modified is not always strong, but changes still invalidate a partial.
    modified = headers.get('Last-Modified')
    return {'header': 'Last-Modified', 'value': modified} if modified else None


def download_resumable(download_url, destination, *, expected_sha256, expected_size,
                       timeout, chunk_size, progress_callback, show_progress,
                       max_retries, retry_backoff, retry_max_delay, record_provenance,
                       force):
    from . import download, provenance
    import requests

    directory = _prepare_directory(destination)
    partial = directory / 'partial'
    metadata_path = directory / 'metadata.json'
    identity = {'sha256': expected_sha256.lower() if expected_sha256 is not None else None,
                'size': expected_size,
                'url_hash': hashlib.sha256(download_url.encode()).hexdigest()}
    with file_lock(directory / 'lock', private=True):
        # Another installer may have completed while we waited for its lock.
        if not force:
            try:
                validate_file(destination, expected_sha256, expected_size)
            except FileNotFoundError:
                pass
            else:
                return
        try:
            metadata = read_json(metadata_path)
        except (OSError, ValueError, RecursionError):
            metadata = {}
        if not isinstance(metadata, dict) or metadata.get('identity') != identity:
            metadata = {'identity': identity, 'validator': None}
            # Unlink, never truncate an unknown path planted in the state dir.
            partial.unlink(missing_ok=True)
        validator = metadata.get('validator')
        valid_validator = _valid_validator(validator)
        strong_validator = (valid_validator and validator['header'] == 'ETag')
        if ((validator is not None and not valid_validator)
                or (expected_sha256 is None and not strong_validator)):
            metadata['validator'] = None
            partial.unlink(missing_ok=True)
        write_json(metadata_path, metadata)
        fd = open_regular(partial, os.O_RDWR | os.O_CREAT, private=True)
        with os.fdopen(fd, 'r+b') as output:
            backoff = retry_backoff
            with Progress(show_progress, 'Downloading', expected_size) as progress:
                def report():
                    done = output.tell()
                    progress(done, expected_size)
                    if progress_callback is not None:
                        progress_callback(done, expected_size)

                for attempt in range(max_retries + 1):
                    output.seek(0, os.SEEK_END)
                    offset = output.tell()
                    if offset >= expected_size:
                        output.flush()
                        if expected_sha256 is not None:
                            try:
                                validate_file(partial, expected_sha256, expected_size)
                            except FileValidationError:
                                pass
                            else:
                                report()
                                break
                        # Size alone cannot validate a completed persistent
                        # partial, including an empty one. Request fresh bytes.
                        output.seek(0)
                        output.truncate()
                        offset = 0
                        metadata['validator'] = None
                    callback_failed = False
                    try:
                        # One protocol restart per attempt, independent of retries.
                        for restart in range(2):
                            headers = {'Accept-Encoding': 'identity', 'User-Agent': USER_AGENT}
                            validator = metadata.get('validator')
                            if offset:
                                headers['Range'] = 'bytes=%d-' % offset
                                if validator and validator['header'] == 'ETag':
                                    headers['If-Range'] = validator['value']
                            with closing(requests.get(download_url, headers=headers,
                                                      timeout=timeout, stream=True)) as response:
                                if offset and response.status_code == 416:
                                    output.seek(0)
                                    output.truncate()
                                    offset = 0
                                    metadata['validator'] = None
                                    continue
                                response.raise_for_status()
                                if response.headers.get('Content-Encoding', 'identity').lower() != 'identity':
                                    raise FileValidationError(destination, 'resume requires identity Content-Encoding')
                                current_validator = _validator(response.headers)
                                if response.status_code == 206:
                                    match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)',
                                                         response.headers.get('Content-Range', ''))
                                    valid_range = (match is not None and
                                                   tuple(map(int, match.groups())) ==
                                                   (offset, expected_size - 1, expected_size))
                                    same_identity = not offset or not validator or validator == current_validator
                                    if not valid_range or not same_identity:
                                        output.seek(0)
                                        output.truncate()
                                        offset = 0
                                        metadata['validator'] = None
                                        if restart == 0:
                                            continue
                                        raise FileValidationError(destination, 'incompatible HTTP Content-Range or validator')
                                elif response.status_code == 200:
                                    output.seek(0)
                                    output.truncate()
                                    offset = 0
                                else:
                                    raise FileValidationError(destination, 'expected HTTP 200 or 206')
                                if expected_sha256 is None and (
                                        current_validator is None or current_validator['header'] != 'ETag'):
                                    raise FileValidationError(
                                        destination, 'resume without expected_sha256 requires a strong ETag')
                                metadata['validator'] = current_validator
                                write_json(metadata_path, metadata)
                                for chunk in response.iter_content(chunk_size=chunk_size):
                                    if not chunk:
                                        continue
                                    if output.tell() + len(chunk) > expected_size:
                                        output.seek(0)
                                        output.truncate()
                                        raise FileValidationError(destination, 'download exceeds expected_size')
                                    output.write(chunk)
                                    try:
                                        report()
                                    except BaseException:
                                        callback_failed = True
                                        raise
                                if output.tell() != expected_size:
                                    raise requests.ConnectionError('incomplete resumable response')
                                break
                        else:
                            raise FileValidationError(destination, 'server could not restart the download')
                        output.flush()
                        try:
                            validate_file(partial, expected_sha256, expected_size, show_progress=show_progress)
                        except FileValidationError:
                            output.seek(0)
                            output.truncate()
                            raise
                        break
                    except BaseException as error:
                        output.flush()
                        if callback_failed or not is_retryable_http_error(error) or attempt == max_retries:
                            raise
                        delay = retry_delay(error, backoff, retry_max_delay)
                        if delay is None:
                            raise
                        logger.warning('Resumable HTTP attempt %d/%d failed (%s); retrying in %.3g seconds',
                                       attempt + 1, max_retries + 1, error_description(error), delay)
                        if delay:
                            time.sleep(delay)
                        backoff *= 2  # retry_delay caps it at retry_max_delay.
                output.flush()
                os.fsync(output.fileno())
        # Keep the resumable inode private even if publication fails after chmod.
        # Copy into ordinary sibling staging; never expose persistent partials.
        staged = None
        try:
            with download._open_staging_file(directory=Path(destination).parent) as handle:
                staged = handle.name
                with partial.open('rb') as source:
                    download.copyfileobj(source, handle)
            record = (provenance.describe(download_url, os.stat(staged), expected_sha256,
                                          transport=metadata.get('validator'))
                      if record_provenance else None)
            download._publish_file(staged, destination, record)
        finally:
            if staged is not None:
                download._remove_staging_file(staged)
        partial.unlink()
        metadata_path.unlink(missing_ok=True)
