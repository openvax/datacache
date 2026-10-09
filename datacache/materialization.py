"""Transactional caller-built artifacts with durable raw-input dependencies."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import url2pathname
from uuid import uuid4

from ._filesystem import open_regular, path_present, read_json, write_json
from .bundle_store import (
    CHUNK_SIZE, INVALID_STORE_ERRORS, MANIFEST, BundleStore, check_expected, check_tree,
    discard_private_directory, hash_file, local_file_identity, private_directory,
    parent_directories, require_directory, validate_distinct_paths, validate_file_record,
    validate_no_sidecar_collisions, validate_relative_name,
)
from .download import normal_creation_mode, validate_download_options
from .integrity import FileValidationError, _validate_expectations
from .progress import Progress
from . import provenance

INPUTS = '.datacache-inputs.json'
FORMAT = 1


def _json_copy(value):
    # Reject non-JSON identities and NaNs, and detach from caller mutation.
    return json.loads(json.dumps(value, sort_keys=True, allow_nan=False))


def _fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def _inventory(mapping):
    if not isinstance(mapping, dict) or not mapping:
        raise ValueError('sources and outputs must be nonempty mappings')
    for name in mapping:
        validate_relative_name(name)
    validate_distinct_paths(mapping)
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
            origin = local_file_identity(spec['path'])
            acquisition[name] = ('path', str(Path(spec['path']).absolute()))
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
    validate_no_sidecar_collisions(normalized)
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


def _source_records(value, definition):
    if not isinstance(value, dict) or set(value) != set(definition):
        raise ValueError('source receipt inventory disagrees with definition')
    for name, record in value.items():
        if not isinstance(record, dict):
            raise ValueError('invalid source receipt')
        observed = {key: record.get(key) for key in ('sha256', 'size')}
        check_expected(name, validate_file_record(observed), definition[name])
        trusted = definition[name]['sha256'] is not None
        if (record.get('identity') != definition[name]['identity'] or
                record.get('origin') != definition[name]['origin'] or
                record.get('verified') is not trusted or
                not isinstance(record.get('fetched_at'), str)):
            raise ValueError('source receipt identity or trust disagrees with definition')
        transport = record.get('transport')
        if transport is not None:
            from .resume import _valid_validator
            if not _valid_validator(transport):
                raise ValueError('invalid validated transport metadata')
    return value


@dataclass(frozen=True)
class MaterializationInspection:
    """Read-only status of the current bundle of built outputs, with its sources.

    status is available, missing, invalid or inaccessible. bundle is the
    current bundle's directory. verified refers only to outputs matched against caller-trusted hashes now.
    Source records distinguish acquisition-time trusted hashes from observation.
    """
    path: str
    status: str
    verified: bool = False
    bundle: object = None
    files: dict = field(default_factory=dict)
    sources: dict = field(default_factory=dict)
    transform: object = None
    error: object = None


def _check_bundle(store, directory, expected=None, *, verify_files=True):
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
    for name, record in outputs.items():
        check_expected(name, validate_file_record(record), definition['outputs'][name])
    trusted = expected['outputs'] if expected is not None else None
    hashed = check_tree(directory, outputs, parent_directories(outputs), ignore=(MANIFEST,),
                        hash_contents=verify_files)
    # Only a hash the caller supplied verifies an output; the receipt's own doesn't.
    files = {name: value.inspection(verified=bool(verify_files and trusted and trusted[name]['sha256']))
             for name, value in hashed.items()}
    verified = verify_files and trusted is not None and all(spec['sha256'] for spec in trusted.values())
    return MaterializationInspection(str(store.path), 'available', bool(verified), str(directory),
                                      files, sources, definition['transform'])


def inspect_materialization(destination, sources=None, *, transform=None, outputs=None, verify_files=True):
    """Check the current bundle offline, without writes, locks or source access.

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
    store = BundleStore(path, 'materialization')
    try:
        bundle = store.current_bundle()
        if bundle is None:
            return MaterializationInspection(str(path), 'missing')
        return _check_bundle(store, bundle, expected, verify_files=verify_files)
    except PermissionError as error:
        return MaterializationInspection(str(path), 'inaccessible', error=error)
    except INVALID_STORE_ERRORS as error:
        return MaterializationInspection(str(path), 'invalid', error=error)


def _input_directory(store, definition):
    return store / ('.inputs-%s-%d' % (_fingerprint(definition['sources']), os.getuid()))


def _cleanup_inputs(store, definition, retain_sources):
    if not retain_sources:
        discard_private_directory(_input_directory(store, definition))


def _acquire_inputs(directory, definition, acquisition, options, *, force=False):
    from .download import fetch_file
    private_directory(directory)
    receipt_path = directory / INPUTS
    try:
        records = read_json(receipt_path, limit=None) if path_present(receipt_path) else {}
        if not isinstance(records, dict) or set(records) - set(definition):
            raise FileValidationError(receipt_path, 'invalid private input receipt')
    except (ValueError, RecursionError) as error:
        if not force:
            raise FileValidationError(
                receipt_path, 'invalid private input receipt; use force=True to repair') from error
        # Unrecorded inputs are checked or acquired again below.
        records = {}
        write_json(receipt_path, records)
    paths = {}
    for name, spec in definition.items():
        target = directory / name
        for parent in reversed(target.relative_to(directory).parents):
            current = directory / parent
            if not path_present(current):
                current.mkdir(mode=0o700)
            require_directory(current)
        kind, source = acquisition[name]
        if force and kind == 'path' and name in records and path_present(Path(source)):
            # The caller may have corrected a local source in place without
            # changing its path, so a forced refresh copies it again while the
            # original exists; once it's gone, the retained copy still serves.
            del records[name]
            write_json(receipt_path, records)
            if path_present(target):
                os.close(open_regular(target))  # Never remove a planted link/special file.
                target.unlink()
            provenance.remove(target)
        if name in records:
            try:
                _source_records({name: records[name]}, {name: spec})
                hash_file(target, records[name])
            except (FileNotFoundError, ValueError) as error:
                if not force:
                    raise FileValidationError(
                        target, 'invalid private input; use force=True to repair') from error
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
                    hash_file(target, spec)
                except FileValidationError:
                    if not force:
                        raise FileValidationError(target, 'invalid private input; use force=True to repair')
                    target.unlink()
                    provenance.remove(target)
        copied = None
        if kind == 'url':
            fetch_file(source, destination=target, raw=True, record_provenance=True,
                       expected_sha256=spec['sha256'], expected_size=spec['size'], **options)
        elif not path_present(target):
            temporary = directory / ('.copy-' + uuid4().hex)
            try:
                with os.fdopen(open_regular(source), 'rb') as handle, temporary.open('xb') as output:
                    digest = hashlib.sha256()
                    with Progress(options['show_progress'], 'Copying', os.fstat(handle.fileno()).st_size) as progress:
                        size = 0
                        for chunk in iter(lambda: handle.read(options.get('chunk_size', CHUNK_SIZE)), b''):
                            output.write(chunk)
                            digest.update(chunk)  # Hash while copying: one read.
                            size += len(chunk)
                            progress(size, progress.total)
                            if options.get('progress_callback') is not None:
                                options['progress_callback'](size, progress.total)
                    output.flush()
                    os.fsync(output.fileno())
                copied = dict(sha256=digest.hexdigest(), size=size)
                check_expected(temporary, copied, spec)
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
        if copied is not None:
            observed, info = copied, os.stat(target)
        else:
            hashed = hash_file(target, spec, show_progress=options['show_progress'])
            observed, info = hashed.record, hashed.info
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
    """Build all declared outputs, then publish them together as a new bundle.

    builder(source_paths, output_paths) must create/close exactly the declared
    outputs and leave inputs unchanged. Source definitions contain url or path
    plus optional raw sha256/size; outputs map names to optional sha256/size.
    transform is {version: nonempty string, options: opaque JSON value}.
    Changed dependencies and a corrupt bundle require explicit force=True.
    Force rebuilds outputs while reusing any complete matching private inputs.

    Inputs survive failures, including without HTTP resume. resume=True enables
    existing raw HTTP resume rules for remote sources; local inputs are copied.
    Successful publication removes owned inputs unless retain_sources=True.
    Caller-owned inputs and old bundles are never deleted. Cache hits are
    offline and read-only; verify_files=False skips payload hashing only on
    reuse. New outputs are always hashed before publication.
    Download/copy/verification progress is on by default; builder progress is
    caller-owned. Installation requires POSIX locks and atomic local renames.
    """
    if not all(isinstance(value, bool) for value in (force, retain_sources, verify_files)):
        raise ValueError('force, retain_sources and verify_files must be booleans')
    if not callable(builder):
        raise ValueError('builder must be callable')
    definition, acquisition = _definition(sources, transform, outputs)
    options = validate_download_options(download_options, 'materialization')
    options.setdefault('show_progress', True)
    if options.get('resume'):
        from .resume import validate_resume
        for name, (kind, source) in acquisition.items():
            if kind == 'url':
                validate_resume(source, definition['sources'][name]['sha256'], definition['sources'][name]['size'])
    path = Path(destination).absolute()
    store = BundleStore(path, 'materialization')

    def inspect_current():
        # A forced refresh rebuilds whatever is there; it only needs to know
        # whether the store is readable, not to hash the old outputs.
        return _inspect_materialization(path, definition, verify_files=verify_files and not force)

    inspection = inspect_current()
    if store.can_reuse(inspection, force=force):
        return {name: value.path for name, value in inspection.files.items()}
    if os.name != 'posix':
        raise NotImplementedError('Materialization requires a POSIX local filesystem')
    path.parent.mkdir(parents=True, exist_ok=True)
    with store.lock():
        store.create()
        inspection = inspect_current()
        if store.can_reuse(inspection, force=force):
            return {name: value.path for name, value in inspection.files.items()}
        inputs = _input_directory(path, definition)
        source_paths, records = _acquire_inputs(inputs, definition['sources'], acquisition, options, force=force)
        working = path / ('.staging-%s-%d' % (_fingerprint(definition), os.getuid()))
        # Start from nothing: discard this user's own outputs from a failed build.
        discard_private_directory(working)
        private_directory(working)
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
                hash_file(source, records[name])
            # Reject extras/links/specials before hashing any declared output.
            declared = definition['outputs']
            check_tree(staged, declared, parent_directories(declared), hash_contents=False)
            mode = normal_creation_mode(staged)
            observed = {}
            for name, target in output_paths.items():
                observed[name] = hash_file(target, declared[name], show_progress=options['show_progress']).record
                os.chmod(target, mode)
            receipt = dict(format=FORMAT, created_at=datetime.now(timezone.utc).isoformat(),
                           definition=definition, sources=records, outputs=observed)
            write_json(staged / MANIFEST, receipt, mode=mode)
            # The outputs were just hashed into the manifest; check its
            # structure, inventory and sizes without hashing them again.
            _check_bundle(store, staged, definition, verify_files=False)
            bundle = store.publish(staged)
            _cleanup_inputs(path, definition, retain_sources)
            return {name: str(bundle / name) for name in declared}
        finally:
            discard_private_directory(working)
