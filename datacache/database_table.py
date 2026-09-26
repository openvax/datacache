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

import datetime
import warnings

import numpy as np
import pandas as pd

from .database import fold_identifier
from .database_types import db_type

# pd.NA was introduced after the original supported pandas versions. Missing
# nullable scalars only exist when that feature is installed.
_PANDAS_NA = getattr(pd, "NA", object())


def _sqlite_value(value):
    """Keep scalar types and integer precision; turn missing values into NULL."""
    if value is None or value is _PANDAS_NA or value is pd.NaT:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and pd.isnull(value):
        return None
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    return value

class DatabaseTable(object):
    """Converts between a DataFrame and a sqlite3 database table"""

    def __init__(
            self,
            name,
            column_types,
            make_rows,
            indices=None,
            nullable=None,
            primary_key=None,
            row_count=None):
        self.name = name
        self.column_types = column_types
        self.make_rows = make_rows
        self.indices = [] if indices is None else indices
        self.nullable = set() if nullable is None else nullable
        self.primary_key = primary_key
        self.row_count = row_count

    @property
    def rows(self):
        """Delay constructing list of row tuples"""
        return list(self.iter_rows())

    def iter_rows(self):
        """Iterate rows without materializing the full table."""
        return iter(self.make_rows())

    @classmethod
    def from_dataframe(cls, name, df, indices, primary_key=None):
        """Infer types and normalize column/constraint names consistently."""

        if len(df.columns) == 0 or not all(isinstance(column, str) and column for column in df.columns):
            raise ValueError("DataFrame columns must be non-empty strings")
        normalized = [column.replace(" ", "_") for column in df.columns]
        if len(set(fold_identifier(column) for column in normalized)) != len(normalized):
            raise ValueError("DataFrame column names collide after normalization")
        names = dict(zip(df.columns, normalized))

        def column_name(column):
            # Accept original names and their legacy normalized spellings.
            if column in names:
                return names[column]
            if column in normalized:
                return column
            raise ValueError("Unknown column %r in table %r" % (column, name))

        primary_key = column_name(primary_key) if primary_key is not None else None
        indices = list(indices)
        if any(isinstance(group, str) for group in indices):
            raise ValueError("Each index must be a sequence of column names, not a string")
        indices = [tuple(column_name(column) for column in group) for group in indices]
        if any(not group for group in indices):
            raise ValueError("Index column groups must be non-empty")

        # ordered list (column_name, column_type) pairs
        column_types = []
        # which columns have nullable values
        nullable = set()

        # tag cached database by dataframe's number of rows and columns
        for column_name in df.columns:
            values = df[column_name]
            if values.isnull().any():
                nullable.add(names[column_name])
            column_db_type = db_type(values.dtype)
            column_types.append((column_name.replace(" ", "_"), column_db_type))

        def make_rows():
            # df.values coerces mixed numeric columns to floats and exposes
            # numpy integers that sqlite3 binds as blobs. Read column scalars.
            # Column iteration also works on pandas versions predating the
            # name=None option of itertuples; zip remains lazy on Python 3.
            return (tuple(_sqlite_value(value) for value in row)
                    for row in zip(*(df[column] for column in df.columns)))

        return cls(
            name=name,
            column_types=column_types,
            make_rows=make_rows,
            indices=indices,
            nullable=nullable,
            primary_key=primary_key,
            row_count=len(df))

    @classmethod
    def from_fasta_dict(cls, name, fasta_dict, key_column, value_column):
        """Deprecated: build a table from identifiers mapped to sequence records.

        Parse FASTA in the consuming library, then use db_from_dataframe. This
        helper will be removed in datacache 2.0.
        """
        warnings.warn(
            "DatabaseTable.from_fasta_dict is deprecated and will be removed in "
            "datacache 2.0; build a DataFrame and use db_from_dataframe instead.",
            DeprecationWarning,
            stacklevel=2)
        key_list = list(fasta_dict.keys())
        key_set = set(key_list)
        if len(key_set) != len(key_list):
            raise ValueError(
                "FASTA file contains %d non-unique sequence identifiers" %
                (len(key_list) - len(key_set)))
        column_types = [(key_column, "TEXT"), (value_column, "TEXT")]

        def make_rows():
            return [
                (idx, str(record.seq))
                for (idx, record)
                in fasta_dict.items()
            ]

        return cls(
            name=name,
            column_types=column_types,
            make_rows=make_rows,
            primary_key=key_column)
