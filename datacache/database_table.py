# Copyright (c) 2015. Mount Sinai School of Medicine
#
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

from .database_types import db_type

class DatabaseTable(object):
    """Converts between a DataFrame and a sqlite3 database table"""

    def __init__(
            self,
            name,
            column_types,
            make_rows,
            indices=[],
            nullable=set(),
            primary_key=None):
        self.name = name
        self.column_types = column_types
        self.make_rows = make_rows
        self.indices = indices
        self.nullable = nullable
        self.primary_key = primary_key

    @property
    def rows(self):
        """Delay constructing list of row tuples"""
        return self.make_rows()

    @classmethod
    def from_dataframe(cls, name, df, indices, primary_key=None):
        """Infer table metadata from a DataFrame"""

        # ordered list (column_name, column_type) pairs
        column_types = []
        # which columns have nullable values
        nullable = set()

        # tag cached database by dataframe's number of rows and columns
        for column_name in df.columns:
            values = df[column_name]
            if values.isnull().any():
                nullable.add(column_name)
            column_db_type = db_type(values.dtype)
            column_types.append((column_name.replace(" ", "_"), column_db_type))

        def make_rows():
            return list(tuple(row) for row in df.values)

        return cls(
            name=name,
            column_types=column_types,
            make_rows=make_rows,
            indices=indices,
            nullable=nullable,
            primary_key=primary_key)

    @classmethod
    def from_fasta_dict(cls, name, fasta_dict, key_column, value_column):
        key_list = list(fasta_dict.keys())
        key_set = set(key_list)
        assert len(key_set) == len(key_list), \
            "FASTA file from contains %d non-unique sequence identifiers" % \
            (len(key_list) - len(key_set))
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
