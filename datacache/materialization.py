"""Transactional caller-built artifacts with durable raw-input dependencies."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
from urllib.parse import urlsplit
from urllib.request import url2pathname
from uuid import uuid4

from ._filesystem import file_lock, open_regular, path_present, read_json, write_json
from .bundles import _directory, _file_mode, _generation, _initialize, _relative_name, _store
from .inspection import FileInspection
from .integrity import FileValidationError, _validate_expectations
from .progress import Progress
from . import provenance

STORE = '.datacache-materialization.json'
MANIFEST = '.datacache-manifest.json'
CURRENT = 'current.json'
INPUTS = '.datacache-inputs.json'
FORMAT = 1
CHUNK_SIZE = 2 ** 20


def _json_copy(value):
    # Reject non-JSON identities and NaNs, and detach from caller mutation.
    return json.loads(json.dumps(value, sort_keys=True, allow_nan=False))


def _fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def _inventory(mapping):
    if not isinstance(mapping, dict) or not mapping:
        raise ValueError('sources and outputs must be nonempty mappings')
    for name in mapping:
        _relative_name(name)
    folded = {name.casefold() for name in mapping}
    parents = {'/'.join(name.split('/')[:i]).casefold()
               for name in mapping for i in range(1, len(name.split('/')))}
    if len(folded) != len(mapping) or folded & parents:
        raise ValueError('paths collide as files/directories or ignoring case')
    return mapping


def _expectations(spec):
    if not isinstance(spec, dict) or set(spec) - {'sha256', 'size'}:
        raise ValueError('output expectations must contain only sha256 and size')
    digest, size = spec.get('sha256'), spec.get('size')
    _validate_expectations(digest, size)
    return dict(sha256=digest.lower() if digest else None, size=size)


def _transform(value):
    if (not isinstance(value, dict) or set(value) - {'version', 'options'} or
            not isinstance(value.get('version'), str) or not value['version']):
        raise ValueError('transform requires a nonempty version and optional JSON options')
    return dict(version=value['version'], options=_json_copy(value.get('options', {})))


def _definition(sources, transform, outputs):
    normalized, acquisition = {}, {}
    for name, spec in _inventory(sources).items():
        if not isinstance(spec, dict) or set(spec) - {'url', 'path', 'sha256', 'size'}:
            raise ValueError('source metadata supports url or path, sha256 and size')
        if ('url' in spec) == ('path' in spec):
            raise ValueError('each source requires exactly one of url or path')
        expected = _expectations({key: spec[key] for key in ('sha256', 'size') if key in spec})
        if 'path' in spec:
            source = Path(spec['path']).absolute()
            origin = source.as_uri()
            acquisition[name] = ('path', str(source))
        else:
            origin = spec['url']
            if not isinstance(origin, str) or not origin:
                raise ValueError('source URL must be a nonempty string')
            parts = urlsplit(origin)
            if parts.scheme not in ('http', 'https', 'ftp', 'file'):
                raise ValueError('source URL must use HTTP(S), FTP or file')
            if parts.scheme == 'file':
                if parts.netloc not in ('', 'localhost') or parts.query or parts.fragment:
                    raise ValueError('local file URLs cannot have remote hosts, queries or fragments')
                acquisition[name] = ('path', str(Path(url2pathname(parts.path)).absolute()))
            else:
                acquisition[name] = ('url', origin)
        normalized[name] = dict(expected, identity=_fingerprint(origin), origin=provenance.redact_url(origin))
    sidecars = {Path(provenance.sidecar_path(name)).as_posix().casefold() for name in normalized}
    occupied = {name.casefold() for name in normalized}
    parents = {'/'.join(name.split('/')[:i]).casefold() for name in normalized
               for i in range(1, len(name.split('/')))}
    if sidecars & (occupied | parents):
        raise ValueError('source paths collide with automatic provenance sidecars')
    output_specs = {name: _expectations(spec) for name, spec in _inventory(outputs).items()}
    return dict(sources=normalized, transform=_transform(transform), outputs=output_specs), acquisition


def _recorded_definition(value):
    if not isinstance(value, dict) or set(value) != {'sources', 'transform', 'outputs'}:
        raise ValueError('invalid materialization dependency definition')
    for spec in _inventory(value['sources']).values():
        if not isinstance(spec, dict) or set(spec) != {'origin', 'identity', 'sha256', 'size'}:
            raise ValueError('invalid source dependency definition')
        _expectations({key: spec[key] for key in ('sha256', 'size')})
        _validate_expectations(spec['identity'], None)
        if spec['identity'] is None or not isinstance(spec['origin'], str):
            raise ValueError('source identity and origin are required')
    for spec in _inventory(value['outputs']).values():
        _expectations(spec)
    if _transform(value['transform']) != value['transform']:
        raise ValueError('invalid transform definition')
    return value


def _check_observed(path, observed, expected):
    if not isinstance(observed, dict) or set(observed) != {'sha256', 'size'}:
        raise ValueError('invalid observed file metadata')
    _validate_expectations(observed['sha256'], observed['size'])
    if observed['sha256'] is None or observed['size'] is None:
        raise ValueError('observed hashes and sizes are required')
    for key in ('sha256', 'size'):
        if expected[key] is not None and observed[key] != expected[key]:
            raise FileValidationError(path, '%s disagrees with dependency expectations' % key)


def _observe(path, expected=None, *, verify_files=True, show_progress=False):
    with os.fdopen(open_regular(path), 'rb') as handle:
        info = os.fstat(handle.fileno())
        if verify_files:
            digest, size = hashlib.sha256(), 0
            with Progress(show_progress, 'Verifying', info.st_size) as progress:
                for chunk in iter(lambda: handle.read(CHUNK_SIZE), b''):
                    digest.update(chunk)
                    size += len(chunk)
                    progress(size, info.st_size)
            if size != info.st_size or os.fstat(handle.fileno()).st_mtime_ns != info.st_mtime_ns:
                raise FileValidationError(path, 'file changed during verification')
            observed = dict(sha256=digest.hexdigest(), size=size)
            if expected is not None:
                _check_observed(path, observed, expected)
        else:
            observed = dict(sha256=None, size=info.st_size)
            if expected is not None and expected['size'] is not None and info.st_size != expected['size']:
                raise FileValidationError(path, 'file size disagrees with receipt')
    return observed, info


def _source_records(value, definition):
    if not isinstance(value, dict) or set(value) != set(definition):
        raise ValueError('source receipt inventory disagrees with definition')
    for name, record in value.items():
        if not isinstance(record, dict):
            raise ValueError('invalid source receipt')
        observed = {key: record.get(key) for key in ('sha256', 'size')}
        _check_observed(name, observed, definition[name])
        trusted = definition[name]['sha256'] is not None
        if (record.get('identity') != definition[name]['identity'] or
                record.get('origin') != definition[name]['origin'] or
                record.get('verified') is not trusted or
                not isinstance(record.get('fetched_at'), str)):
            raise ValueError('source receipt identity or trust disagrees with definition')
        transport = record.get('transport')
        if transport is not None:
            from .resume import _strong_etag
            if (not isinstance(transport, dict) or set(transport) != {'header', 'value'} or
                    transport['header'] not in ('ETag', 'Last-Modified') or
                    not isinstance(transport['value'], str) or
                    '\r' in transport['value'] or '\n' in transport['value'] or
                    (transport['header'] == 'ETag' and not _strong_etag(transport['value']))):
                raise ValueError('invalid validated transport metadata')
    return value


def _tree(root, recorded, trusted=None, *, verify_files=True, show_progress=False, allow_manifest=True):
    """Check the exact inventory without following directories or special files."""
    files, found, stack = {}, set(), [(Path(root), '')]
    parents = {'/'.join(name.split('/')[:i]) for name in recorded
               for i in range(1, len(name.split('/')))}
    while stack:
        directory, prefix = stack.pop()
        _directory(directory)
        with os.scandir(directory) as entries:
            for entry in entries:
                name = prefix + entry.name
                if allow_manifest and not prefix and name == MANIFEST:
                    continue
                _relative_name(name)
                if entry.is_dir(follow_symlinks=False):
                    if name not in parents:
                        raise FileValidationError(entry.path, 'undeclared output directory')
                    stack.append((Path(entry.path), name + '/'))
                    continue
                if name not in recorded:
                    raise FileValidationError(entry.path, 'undeclared output file')
                observed, info = _observe(entry.path, recorded[name], verify_files=verify_files,
                                          show_progress=show_progress)
                found.add(name)
                files[name] = FileInspection(entry.path, 'available',
                    verified=bool(verify_files and trusted and trusted[name]['sha256']),
                    size=info.st_size, mtime=info.st_mtime)
    if found != set(recorded):
        raise FileValidationError(root, 'missing declared outputs: %s' % sorted(set(recorded) - found))
    return files


@dataclass(frozen=True)
class MaterializationInspection:
    """One read-only snapshot, including source provenance and transform identity.

    verified refers only to outputs matched against caller-trusted hashes now.
    Source records distinguish acquisition-time trusted hashes from observation.
    """
    path: str
    status: str
    verified: bool = False
    generation: object = None
    files: dict = field(default_factory=dict)
    sources: dict = field(default_factory=dict)
    transform: object = None
    error: object = None


def _inspect_tree(store, directory, expected=None, *, verify_files=True, show_progress=False):
    receipt = read_json(directory / MANIFEST, limit=None)
    if not isinstance(receipt, dict) or receipt.get('format') != FORMAT:
        raise ValueError('unrecognized materialization receipt')
    definition = _recorded_definition(receipt.get('definition'))
    if expected is not None and definition != expected:
        raise FileValidationError(directory, 'dependency identity changed; use force=True to refresh')
    sources = _source_records(receipt.get('sources'), definition['sources'])
    outputs = receipt.get('outputs')
    if not isinstance(outputs, dict) or set(outputs) != set(definition['outputs']):
        raise ValueError('output receipt inventory disagrees with definition')
    for name, observed in outputs.items():
        _check_observed(name, observed, definition['outputs'][name])
    trusted = expected['outputs'] if expected is not None else None
    files = _tree(directory, outputs, trusted, verify_files=verify_files, show_progress=show_progress)
    verified = verify_files and trusted is not None and all(spec['sha256'] for spec in trusted.values())
    return MaterializationInspection(str(store), 'available', bool(verified), str(directory),
                                      files, sources, definition['transform'])


def inspect_materialization(destination, sources=None, *, transform=None, outputs=None, verify_files=True):
    """Inspect offline without writes, locks, source access or automatic recovery.

    Supply all of sources/transform/outputs to check dependency identity; omit
    all three for receipt-only consistency checks. Metadata-only inspection
    checks regular files and sizes, never claims verification of payload hashes.
    """
    if not isinstance(verify_files, bool):
        raise ValueError('verify_files must be a boolean')
    supplied = sum(value is not None for value in (sources, transform, outputs))
    if supplied not in (0, 3):
        raise ValueError('provide sources, transform and outputs together, or omit all three')
    expected = _definition(sources, transform, outputs)[0] if supplied else None
    return _inspect_materialization(destination, expected, verify_files=verify_files)


def _inspect_materialization(destination, expected, *, verify_files=True):
    path = Path(destination)
    try:
        if not path_present(path):
            return MaterializationInspection(str(path), 'missing')
        try:
            _store(path, marker=STORE)
        except FileNotFoundError:
            _directory(path)
            if not any(path.iterdir()):
                return MaterializationInspection(str(path), 'missing')
            _store(path, marker=STORE)
        try:
            pointer = read_json(path / CURRENT)
        except FileNotFoundError:
            status = 'recovery-required' if any((path / 'generations').iterdir()) else 'missing'
            return MaterializationInspection(str(path), status)
        return _inspect_tree(path, _generation(path, pointer['generation']), expected, verify_files=verify_files)
    except PermissionError as error:
        return MaterializationInspection(str(path), 'inaccessible', error=error)
    except (OSError, ValueError, KeyError, TypeError, RecursionError) as error:
        return MaterializationInspection(str(path), 'invalid', error=error)


def _private(path):
    if not path_present(path):
        path.mkdir(mode=0o700)
    info = path.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or
            stat.S_IMODE(info.st_mode) & 0o077):
        raise FileValidationError(path, 'working directory must be private and owner-owned')


def _input_directory(store, definition):
    return store / ('.inputs-%s-%d' % (_fingerprint(definition['sources']), os.getuid()))


def _cleanup_inputs(store, definition, retain_sources):
    directory = _input_directory(store, definition)
    if not retain_sources and path_present(directory):
        _private(directory)
        shutil.rmtree(directory)


def _acquire_inputs(directory, definition, acquisition, options, *, force=False):
    from .download import fetch_file
    _private(directory)
    receipt_path = directory / INPUTS
    records = read_json(receipt_path, limit=None) if path_present(receipt_path) else {}
    if not isinstance(records, dict) or set(records) - set(definition):
        raise FileValidationError(receipt_path, 'invalid private input receipt')
    paths = {}
    for name, spec in definition.items():
        target = directory / name
        for parent in reversed(target.relative_to(directory).parents):
            current = directory / parent
            if not path_present(current):
                current.mkdir(mode=0o700)
            _directory(current)
        if name in records:
            _source_records({name: records[name]}, {name: spec})
            try:
                _observe(target, {key: records[name][key] for key in ('sha256', 'size')})
            except (FileNotFoundError, FileValidationError):
                if not force:
                    raise FileValidationError(target, 'invalid private input; use force=True to repair')
                if path_present(target):
                    fd = open_regular(target)  # Never repair a planted link/special file.
                    os.close(fd)
                    target.unlink()
                provenance.remove(target)
                del records[name]
                write_json(receipt_path, records)
            else:
                paths[name] = str(target)
                continue
        if path_present(target):
            # Only installer-created, private completed bytes can exist here.
            # Validate their type before fetch_file's ordinary cache-hit path.
            fd = open_regular(target)
            os.close(fd)
            # A hash-pinned acquisition is independently reusable. A bare
            # size-only/unverified file has no durable completion receipt yet.
            if spec['sha256'] is None:
                target.unlink()
            else:
                try:
                    _observe(target, spec)
                except FileValidationError:
                    if not force:
                        raise FileValidationError(target, 'invalid private input; use force=True to repair')
                    target.unlink()
                    provenance.remove(target)
        kind, source = acquisition[name]
        if kind == 'url':
            fetch_file(source, destination=target, raw=True, record_provenance=True,
                       expected_sha256=spec['sha256'], expected_size=spec['size'], **options)
        elif not path_present(target):
            temporary = directory / ('.copy-' + uuid4().hex)
            try:
                with os.fdopen(open_regular(source), 'rb') as handle, temporary.open('xb') as output:
                    with Progress(options['show_progress'], 'Copying', os.fstat(handle.fileno()).st_size) as progress:
                        size = 0
                        for chunk in iter(lambda: handle.read(CHUNK_SIZE), b''):
                            output.write(chunk)
                            size += len(chunk)
                            progress(size, progress.total)
                    output.flush()
                    os.fsync(output.fileno())
                _observe(temporary, spec, show_progress=options['show_progress'])
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
        observed, info = _observe(target, spec, show_progress=options['show_progress'])
        origin_record = provenance.read(target, info) or {}
        record = dict(observed, origin=spec['origin'], identity=spec['identity'],
                      verified=spec['sha256'] is not None,
                      fetched_at=origin_record.get('fetched_at', datetime.now(timezone.utc).isoformat()))
        if origin_record.get('transport') is not None:
            record['transport'] = origin_record['transport']
        records[name] = record
        write_json(receipt_path, records)
        paths[name] = str(target)
    return paths, records


def materialize(destination, sources, *, transform, outputs, builder, force=False,
                retain_sources=False, verify_files=True, download_options=None):
    """Build all declared outputs and their receipt as one immutable generation.

    builder(source_paths, output_paths) must create/close exactly the declared
    outputs and leave inputs unchanged. Source definitions contain url or path
    plus optional raw sha256/size; outputs map names to optional sha256/size.
    transform is {version: nonempty string, options: opaque JSON value}.
    Changed dependencies and corrupt generations require explicit force=True.
    Force rebuilds outputs while reusing any complete matching private inputs.

    Inputs survive failures, including without HTTP resume. resume=True enables
    existing raw HTTP resume rules for remote sources; local inputs are copied.
    Successful publication removes owned inputs unless retain_sources=True.
    Caller-owned inputs and old output generations are never deleted. Cache
    hits are offline and read-only; verify_files=False skips payload hashing
    only on reuse. New publication and explicit recovery always check bytes.
    Download/copy/verification progress is on by default; builder progress is
    caller-owned. Installation requires POSIX locks and atomic local renames.
    """
    if not all(isinstance(value, bool) for value in (force, retain_sources, verify_files)):
        raise ValueError('force, retain_sources and verify_files must be booleans')
    if not callable(builder):
        raise ValueError('builder must be callable')
    definition, acquisition = _definition(sources, transform, outputs)
    options = dict(download_options or {})
    allowed = {'timeout', 'chunk_size', 'progress_callback', 'show_progress',
               'max_retries', 'retry_backoff', 'retry_max_delay', 'resume'}
    if set(options) - allowed:
        raise ValueError('unsupported materialization download options')
    options.setdefault('show_progress', True)
    if not isinstance(options['show_progress'], bool) or not isinstance(options.get('resume', False), bool):
        raise ValueError('show_progress and resume must be booleans')
    from .retries import validate_retry_options
    from .download import DEFAULT_CHUNK_SIZE, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BACKOFF, DEFAULT_RETRY_MAX_DELAY
    validate_retry_options(options.get('max_retries', DEFAULT_MAX_RETRIES),
                           options.get('retry_backoff', DEFAULT_RETRY_BACKOFF),
                           options.get('retry_max_delay', DEFAULT_RETRY_MAX_DELAY))
    chunk_size = options.get('chunk_size', DEFAULT_CHUNK_SIZE)
    if not isinstance(chunk_size, int) or isinstance(chunk_size, bool) or chunk_size <= 0:
        raise ValueError('chunk_size must be a positive integer')
    if options.get('progress_callback') is not None and not callable(options['progress_callback']):
        raise ValueError('progress_callback must be callable')
    if options.get('resume'):
        from .resume import validate_resume
        for name, (kind, source) in acquisition.items():
            if kind == 'url':
                validate_resume(source, definition['sources'][name]['sha256'], definition['sources'][name]['size'])
    path = Path(destination).absolute()

    def inspect_current():
        return _inspect_materialization(path, definition, verify_files=verify_files)

    def check_hit(inspection):
        if inspection.status == 'inaccessible':
            raise inspection.error
        if not force and inspection.status == 'invalid':
            raise FileValidationError(path, 'invalid materialization; use force=True to refresh or repair') from inspection.error
        return not force and inspection.status == 'available'

    inspection = inspect_current()
    if check_hit(inspection):
        return {name: value.path for name, value in inspection.files.items()}
    if os.name != 'posix':
        raise NotImplementedError('Materialization requires a POSIX local filesystem')
    path.parent.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(os.fsencode(path.name)).hexdigest()[:32]
    with file_lock(path.parent / ('.datacache-materialization-lock-' + key)):
        _initialize(path, marker=STORE)
        inspection = inspect_current()
        if check_hit(inspection):
            return {name: value.path for name, value in inspection.files.items()}
        if inspection.status in ('missing', 'recovery-required'):
            for entry in sorted((path / 'generations').iterdir(), reverse=True):
                try:
                    candidate = _inspect_tree(path, _generation(path, entry.name), definition)
                except (OSError, ValueError, KeyError, TypeError, RecursionError):
                    continue
                write_json(path / CURRENT, {'generation': entry.name}, mode=_file_mode(path))
                _cleanup_inputs(path, definition, retain_sources)
                return {name: value.path for name, value in candidate.files.items()}
        inputs = _input_directory(path, definition)
        source_paths, records = _acquire_inputs(inputs, definition['sources'], acquisition, options, force=force)
        working = path / ('.staging-%s-%d' % (_fingerprint(definition), os.getuid()))
        _private(working)
        # Only this user's installer-owned failed outputs are discarded.
        for entry in working.iterdir():
            if entry.is_dir() and not entry.is_symlink():
                shutil.rmtree(entry)
            else:
                entry.unlink()
        staged = working / 'files'
        staged.mkdir()
        output_paths = {name: str(staged / name) for name in definition['outputs']}
        for target in output_paths.values():
            Path(target).parent.mkdir(parents=True, exist_ok=True)
        try:
            builder(dict(source_paths), dict(output_paths))
            # Builder callbacks are trusted code, not a sandbox; enforce their
            # read-only-input contract before creating a dependency receipt.
            for name, source in source_paths.items():
                _observe(source, {key: records[name][key] for key in ('sha256', 'size')})
            observed = {}
            # Reject extras/links/specials before hashing any declared output.
            _tree(staged, definition['outputs'], verify_files=False, allow_manifest=False)
            for name, target in output_paths.items():
                observed[name] = _observe(target, definition['outputs'][name],
                                          show_progress=options['show_progress'])[0]
                os.chmod(target, _file_mode(staged))
            receipt = dict(format=FORMAT, created_at=datetime.now(timezone.utc).isoformat(),
                           definition=definition, sources=records, outputs=observed)
            write_json(staged / MANIFEST, receipt, mode=_file_mode(staged))
            _inspect_tree(path, staged, definition)
            generation = uuid4().hex
            final = path / 'generations' / generation
            os.replace(staged, final)
            # Remap only after validating the receipt in its private staging.
            candidate = _inspect_tree(path, final, definition, verify_files=False)
            write_json(path / CURRENT, {'generation': generation}, mode=_file_mode(path))
            _cleanup_inputs(path, definition, retain_sources)
            return {name: value.path for name, value in candidate.files.items()}
        finally:
            _private(working)
            shutil.rmtree(working)
