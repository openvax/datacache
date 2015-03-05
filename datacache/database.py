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

import logging
import sqlite3

from typechecks import require_integer, require_string, require_iterable_of

METADATA_TABLE_NAME = "_datacache_metadata"

class Database(object):
    """
    Wrapper object for sqlite3 database which provides helpers for
    querying and constructing the datacache metadata table, as well as
    creating and checking for existence of particular table names.

    Calls to methods other than Database.close() and Database.create()
    will not commit their changes.
    """
    def __init__(self, path):
        self.path = path
        self.connection = sqlite3.connect(path)

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
        """Does a table named `table_name` exist in the sqlite database?"""
        table_names = self.table_names()
        return table_name in table_names

    def drop_all_tables(self):
        """Drop all tables in the database"""
        for table_name in self.table_names():
            self.execute_sql("DROP TABLE %s" % table_name)
        self.connection.commit()

    def execute_sql(self, sql, commit=False):
        """Log and then execute a SQL query"""
        logging.info("Running sqlite query: \"%s\"", sql)
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
        query = "SELECT version FROM %s" % METADATA_TABLE_NAME
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
            "CREATE TABLE %s (version INT)" % METADATA_TABLE_NAME
        self.execute_sql(create_metadata_sql)
        insert_version_sql = \
            "INSERT INTO %s VALUES (%s)" % (METADATA_TABLE_NAME, version)
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
            decl = "%s %s" % (column_name, column_type)
            if column_name == primary:
                decl += " UNIQUE PRIMARY KEY"
            if column_name not in nullable:
                decl += " NOT NULL"
            column_decls.append(decl)
        column_decl_str = ", ".join(column_decls)
        create_table_sql = \
            "CREATE TABLE %s (%s)" % (table_name, column_decl_str)
        self.execute_sql(create_table_sql)

    def _fill_table(self, table_name, rows):
        require_string(table_name, "table_name")
        require_iterable_of(rows, tuple, "rows")

        if not self.has_table(table_name):
            raise ValueError(
                "Table '%s' does not exist in database" % (table_name,))
        if len(rows) == 0:
            raise ValueError("Rows must be non-empty sequence")

        first_row = rows[0]
        n_columns = len(first_row)
        if not all(len(row) == n_columns for row in rows):
            raise ValueError("Rows must all have %d values" % n_columns)
        blank_slots = ", ".join("?" for _ in range(n_columns))
        logging.info("Inserting %d rows into table %s", len(rows), table_name)
        sql = "INSERT INTO %s VALUES (%s)" % (table_name, blank_slots)
        self.connection.executemany(sql, rows)

    def create(self, tables, version):
        """Do the actual work of creating the database, filling its tables with
        values, creating indices, and setting the datacache version metadata.

        Parameters
        ----------
        tables : list
            List of datacache.DatabaseTable objects

        version : int
        """
        for table in tables:
            self._create_table(
                table_name=table.name,
                column_types=table.column_types,
                primary=table.primary_key,
                nullable=table.nullable)
            self._fill_table(table.name, table.rows)
            self._create_indices(table.name, table.indices)
        self._finalize_database(version)
        self._commit()

    def _create_index(self, table_name, index_columns):
        """
        Creates an index over multiple columns of a given table.

        Parameters
        ----------
        table_name : str

        index_columns : iterable of str
            Which columns should be indexed
        """

        logging.info("Creating index on %s (%s)" % (
                table_name,
                ", ".join(index_columns)))
        index_name = "%s_index_%s" % (
            table_name,
            "_".join(index_columns))
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS %s ON %s (%s)" % (
                index_name,
                table_name,
                ", ".join(index_columns)))

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
