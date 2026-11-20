"""Test helpers wrapping ``db_from_dataframes_with_absolute_path``."""

from datacache import db_from_dataframes_with_absolute_path
from datacache.common import resolve_path


def db_from_dataframes(
    db_filename,
    dataframes=None,
    *,
    table_names_to_dataframes=None,
    primary_keys=None,
    table_names_to_primary_keys=None,
    indices=None,
    table_names_to_indices=None,
    subdir=None,
    overwrite=False,
    version=1,
    cache_root=None,
    show_progress=False,
    **kwargs,
):
    tables = table_names_to_dataframes if table_names_to_dataframes is not None else dataframes
    pks = table_names_to_primary_keys if table_names_to_primary_keys is not None else primary_keys
    idxs = table_names_to_indices if table_names_to_indices is not None else indices
    return db_from_dataframes_with_absolute_path(
        resolve_path(db_filename, subdir=subdir, cache_root=cache_root),
        table_names_to_dataframes=tables,
        table_names_to_primary_keys=pks,
        table_names_to_indices=idxs,
        overwrite=overwrite,
        version=version,
        show_progress=show_progress,
        **kwargs,
    )


def db_from_dataframe(
    db_filename,
    table_name,
    df,
    indices=None,
    primary_key=None,
    subdir=None,
    overwrite=False,
    version=1,
    cache_root=None,
    show_progress=False,
    **kwargs,
):
    return db_from_dataframes(
        db_filename=db_filename,
        table_names_to_dataframes={table_name: df},
        table_names_to_primary_keys={table_name: primary_key} if primary_key else None,
        table_names_to_indices={table_name: indices} if indices else None,
        subdir=subdir,
        overwrite=overwrite,
        version=version,
        cache_root=cache_root,
        show_progress=show_progress,
        **kwargs,
    )
