# Copyright (c) 2015-2018 Mount Sinai School of Medicine
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

from __future__ import print_function, division, absolute_import

import logging
import sqlite3
import string
from pathlib import Path
from itertools import chain, islice

from typechecks import require_integer, require_string, require_iterable_of

from .progress import Progress


logger = logging.getLogger(__name__)

METADATA_TABLE_NAME = "_datacache_metadata"

_ASCII_LOWERCASE = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)


def fold_identifier(name):
    """Fold an identifier's case the way SQLite compares names: ASCII only.

    SQLite treats "Records" and "records" as one table but "Éclair" and
    "éclair" as two, so str.lower() would match tables SQLite considers
    absent. Non-string values are returned unchanged and match nothing.
    """
    return name.translate(_ASCII_LOWERCASE) if isinstance(name, str) else name


def quote_identifier(identifier):
    """
    Wrap a SQL identifier (table, column, or index name) in double quotes so
    that names which contain spaces or punctuation, or which collide with SQL
    keywords, don't need to be sanitized down to a restricted character set.
    Embedded double quotes are escaped by doubling them, per the SQL standard.

    See https://github.com/openvax/datacache/issues/17
    """
    return '"%s"' % str(identifier).replace('"', '""')


class Database(object):
    """
    Wrapper object for sqlite3 database which provides helpers for
    querying and constructing the datacache metadata table, as well as
    creating and checking for existence of particular table names.

    create() commits a complete build, rolling back on failure. close() commits
    pending work before closing; exception handlers should roll back and close
    the underlying connection directly. drop_all_tables() commits by default.
    """
    def __init__(self, path, *, must_exist=False, read_only=False):
        self.path = path
        # check_same_thread=False allows a cached database connection to be
        # reused across threads, which is needed by long-running / interactive
        # consumers (e.g. pyensembl) that may issue queries from different
        # threads than the one which opened the connection. Applications still
        # need to coordinate simultaneous use of one connection; independent
        # workers should have their own. See https://github.com/openvax/datacache/issues/45
        if must_exist or read_only:
            uri = Path(path).absolute().as_uri() + ("?mode=ro" if read_only else "?mode=rw")
            self.connection = sqlite3.connect(uri, uri=True, check_same_thread=False)
        else:
            self.connection = sqlite3.connect(path, check_same_thread=False)

    def _commit(self):
        self.connection.commit()

    def close(self):
        """Commit changes and close database connection"""
        self._commit()
        self.connection.close()

    def table_names(self):
        """Returns names of all tables in the database"""
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        cursor = self.connection.execute(query)
        results = cursor.fetchall()
        return [result_tuple[0] for result_tuple in results]

    def has_table(self, table_name):
        """Does a table named `table_name` exist, comparing case as SQLite does?"""
        return fold_identifier(table_name) in {
            fold_identifier(name) for name in self.table_names()}

    def drop_all_tables(self, *, commit=True, include_views=False):
        """Drop tables, optionally removing views for a complete overwrite.

        commit=False lets the caller roll back the entire schema replacement.
        Ordinary version rebuilds retain views for compatibility.
        """
        if include_views:
            views = self.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='view'").fetchall()
            for (view_name,) in views:
                self.execute_sql("DROP VIEW %s" % quote_identifier(view_name))
        for table_name in self.table_names():
            if not table_name.startswith("sqlite_"):
                # Dropping a virtual table also drops its shadow tables. They
                # may still appear later in this snapshot of sqlite_master.
                self.execute_sql("DROP TABLE IF EXISTS %s" % quote_identifier(table_name))
        if commit:
            self.connection.commit()

    def execute_sql(self, sql, commit=False):
        """Log and then execute a SQL query"""
        logger.info("Running sqlite query: \"%s\"", sql)
        self.connection.execute(sql)
        if commit:
            self.connection.commit()

    def has_tables(self, table_names):
        """Are all of the given table names present in the database?"""
        return all(self.has_table(table_name) for table_name in table_names)

    def has_version(self):
        """Does this database have version information?

        The absence of version information indicates that this database was
        either not created by datacache or is incomplete.
        """
        return self.has_table(METADATA_TABLE_NAME)

    def version(self):
        """What's the version of this database? Found in metadata attached
        by datacache when creating this database."""
        query = "SELECT %s FROM %s" % (
            quote_identifier("version"),
            quote_identifier(METADATA_TABLE_NAME))
        cursor = self.connection.execute(query)
        version = cursor.fetchone()
        if not version:
            return 0
        else:
            return int(version[0])

    def _finalize_database(self, version):
        """
        Create metadata table for database with version number.

        Parameters
        ----------
        version : int
            Tag created database with user-specified version number
        """
        require_integer(version, "version")
        create_metadata_sql = \
            "CREATE TABLE %s (%s INT)" % (
                quote_identifier(METADATA_TABLE_NAME),
                quote_identifier("version"))
        self.execute_sql(create_metadata_sql)
        insert_version_sql = \
            "INSERT INTO %s VALUES (%s)" % (
                quote_identifier(METADATA_TABLE_NAME), version)
        self.execute_sql(insert_version_sql)

    def _create_table(self, table_name, column_types, primary=None, nullable=()):
        """Creates a sqlite3 table from the given metadata.

        Parameters
        ----------

        column_types : list of (str, str) pairs
            First element of each tuple is the column name, second element is the sqlite3 type

        primary : str, optional
            Which column is the primary key

        nullable : iterable, optional
            Names of columns which have null values
        """
        require_string(table_name, "table name")
        require_iterable_of(column_types, tuple, name="rows")
        if primary is not None:
            require_string(primary, "primary")
        require_iterable_of(nullable, str, name="nullable")

        column_decls = []
        for column_name, column_type in column_types:
            decl = "%s %s" % (quote_identifier(column_name), column_type)
            if column_name == primary:
                decl += " UNIQUE PRIMARY KEY"
            if column_name == primary or column_name not in nullable:
                decl += " NOT NULL"
            column_decls.append(decl)
        column_decl_str = ", ".join(column_decls)
        create_table_sql = \
            "CREATE TABLE %s (%s)" % (
                quote_identifier(table_name), column_decl_str)
        self.execute_sql(create_table_sql)

    def _fill_table(self, table_name, rows, *, show_progress=False, total=None):
        require_string(table_name, "table_name")

        if not self.has_table(table_name):
            raise ValueError(
                "Table '%s' does not exist in database" % (table_name,))
        rows = iter(rows)
        empty = object()
        first_row = next(rows, empty)
        if first_row is empty:
            return
        if not isinstance(first_row, tuple):
            raise TypeError("Rows must be tuples")
        n_columns = len(first_row)
        blank_slots = ", ".join("?" for _ in range(n_columns))
        logger.info("Inserting rows into table %s", table_name)
        sql = "INSERT INTO %s VALUES (%s)" % (
            quote_identifier(table_name), blank_slots)
        rows = chain((first_row,), rows)
        with Progress(show_progress, "Writing " + table_name, total, unit="rows") as progress:
            completed = 0
            while True:
                batch = list(islice(rows, 1000))
                if not batch:
                    break
                if not all(isinstance(row, tuple) and len(row) == n_columns for row in batch):
                    raise ValueError("Rows must all be tuples with %d values" % n_columns)
                self.connection.executemany(sql, batch)
                completed += len(batch)
                progress(completed, total)

    def create(self, tables, version, *, show_progress=False):
        """Do the actual work of creating the database, filling its tables with
        values, creating indices, and setting the datacache version metadata.

        Parameters
        ----------
        tables : list
            List of datacache.DatabaseTable objects

        version : int
        """
        # Explicit BEGIN includes DDL in the transaction on all supported
        # Python sqlite3 versions. A caller's pending drops join this transaction.
        with self.connection:
            if not self.connection.in_transaction:
                self.connection.execute("BEGIN")
            for table in tables:
                self._create_table(
                    table_name=table.name,
                    column_types=table.column_types,
                    primary=table.primary_key,
                    nullable=table.nullable)
                self._fill_table(table.name, table.iter_rows(), show_progress=show_progress, total=table.row_count)
                self._create_indices(table.name, table.indices)
            self._finalize_database(version)

    def _create_index(self, table_name, index_columns):
        """
        Creates an index over multiple columns of a given table.

        Parameters
        ----------
        table_name : str

        index_columns : iterable of str
            Which columns should be indexed
        """

        logger.info(
            "Creating index on %s (%s)",
            table_name,
            ", ".join(index_columns))
        index_columns = tuple(index_columns)
        base_name = "%s_index_%s" % (
            table_name,
            "_".join(index_columns))
        index_name = base_name
        suffix = 2
        table_indices = {row[1]: row[4] for row in self.connection.execute(
            "PRAGMA index_list(%s)" % quote_identifier(table_name))}
        while True:
            existing = self.connection.execute(
                "SELECT name FROM sqlite_master WHERE name = ? COLLATE NOCASE "
                "AND type IN ('table', 'view', 'index')", (index_name,)).fetchone()
            if existing is None:
                break
            existing_name = existing[0]
            if existing_name in table_indices and not table_indices[existing_name]:
                columns = tuple(row[2] for row in self.connection.execute(
                    "PRAGMA index_info(%s)" % quote_identifier(existing_name)))
                if columns == index_columns:
                    return
            # Keep the legacy name when available, but never silently skip a
            # different index (even on another table) that shares that name.
            index_name = "%s_%d" % (base_name, suffix)
            suffix += 1
        self.connection.execute(
            "CREATE INDEX %s ON %s (%s)" % (
                quote_identifier(index_name),
                quote_identifier(table_name),
                ", ".join(quote_identifier(c) for c in index_columns)))

    def _create_indices(self, table_name, indices):
        """
        Create multiple indices (each over multiple columns) on a given table.

        Parameters
        ----------
        table_name : str

        indices : iterable of tuples
            Multiple groups of columns, each of which should be indexed.
        """
        require_string(table_name, "table_name")
        require_iterable_of(indices, (tuple, list))
        for index_column_set in indices:
            self._create_index(table_name, index_column_set)
