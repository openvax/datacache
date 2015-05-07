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
from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData
from sqlalchemy.exc import OperationalError

from typechecks import (require_integer, require_string,
                        require_iterable_of)

METADATA_TABLE_PREFIX = "_datacache_metadata"

class Database(object):
    """
    Wrapper object for a database which provides helpers for
    querying and constructing the datacache metadata table, as well as
    creating and checking for existence of particular table names.

    This "Database" represents a collection that may share a physical
    database with other collections.

    Calls to methods will not commit their changes, other than:
     * Database.create
     * Database.drop_tables
     * Database.drop_collection_tables
    """
    def __init__(self, db_url, collection_name):
        self.engine = create_engine(db_url)
        # Do not autocommit by default
        self.connection = self.engine.connect().execution_options(
            autocommit=False)
        # Bind to the connection rather than the engine
        # so that it reflects the state of this transaction
        self.metadata = MetaData(bind=self.connection, reflect=False)
        self.collection_name = collection_name

    def update_metadata(self):
        """Updates SQLAlchemy metadata (table information)"""
        # Note: this metadata object is totally separate from
        # DataCache's concept of a metadata table.
        self.metadata.reflect()

    def close(self):
        """Close database connection"""
        self.connection.close()

    def table_names(self):
        """Returns names of all tables in the collection"""
        self.update_metadata()
        table_names = self.metadata.tables.keys()
        return [table_name for table_name in table_names if
                table_name.endswith(self.collection_name)]

    def has_table(self, table_name):
        """Does a table named `table_name` exist in the collection?"""
        table_names = self.table_names()
        return table_name in table_names

    def drop_tables(self, table_names):
        """Drop all tables in the argument list"""
        # The context manager commits at the close of the transaction
        with self.connection.begin():
            logging.info("Dropping tables from database: %s",
                         ", ".join(table_names))
            for table_name in table_names:
                try:
                    self.execute_sql("DROP TABLE \"%s\"" % table_name)
                except OperationalError as e:
                    logging.warn("Encountered error %s while trying to "
                                 "drop table \"%s\"" %
                                 (e.message, table_name))

    def execute_sql(self, sql, commit=False):
        """Log and then execute a SQL query"""
        logging.info("Running database query: \"%s\"", sql)
        self.connection.execution_options(
            autocommit=commit).execute(sql)

    def has_tables(self, table_names):
        """Are all of the given table names present in the database?"""
        return all(self.has_table(table_name) for table_name in table_names)

    def has_version(self):
        """Does this database have version information for this collection?

        The absence of version information indicates that this collection was
        either not created by datacache or is incomplete.
        """
        return self.has_table(self.metadata_table_name())

    def version(self):
        """What's the version of this collection? Found in metadata attached
        by datacache when creating this collection."""
        query = "SELECT version FROM \"%s\"" % self.metadata_table_name()
        cursor = self.connection.execute(query)
        version = cursor.fetchone()
        if not version:
            return 0
        else:
            return int(version[0])

    def metadata_table_name(self):
        return "%s_%s" % (METADATA_TABLE_PREFIX, self.collection_name)

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
            "CREATE TABLE \"%s\" (version INT)" % self.metadata_table_name()
        self.execute_sql(create_metadata_sql)
        insert_version_sql = \
            "INSERT INTO \"%s\" VALUES (%s)" % (self.metadata_table_name(), version)
        self.execute_sql(insert_version_sql)

    def _create_table(self, table_name, column_types, primary=None, nullable=()):
        """Creates a database table from the given metadata.

        Parameters
        ----------

        column_types : list of (str, str) pairs
            First element of each tuple is the column name, 
            second element is the database column type

        primary : str, optional
            Which column is the primary key

        nullable : iterable, optional
            Names of columns which have null values
        """
        require_string(table_name, "table name")
        require_iterable_of(column_types, tuple, name="column_types")
        if primary is not None:
            require_string(primary, "primary")
        require_iterable_of(nullable, str, name="nullable")

        column_decls = []
        for column_name, column_type in column_types:
            decl = "\"%s\" %s" % (column_name, column_type)
            if column_name == primary:
                decl += " UNIQUE PRIMARY KEY"
            if column_name not in nullable:
                decl += " NOT NULL"
            column_decls.append(decl)
        column_decl_str = ", ".join(column_decls)
        create_table_sql = \
            "CREATE TABLE \"%s\" (%s)" % (table_name, column_decl_str)
        self.execute_sql(create_table_sql)

    def _fill_table(self, table_name, rows):
        require_string(table_name, "table_name")
        require_iterable_of(rows, dict, "rows")

        if not self.has_table(table_name):
            raise ValueError(
                "Table \"%s\" does not exist in database" % (table_name,))
        if len(rows) == 0:
            raise ValueError("Rows must be non-empty sequence")

        first_row = rows[0]
        n_columns = len(first_row)
        if not all(len(row) == n_columns for row in rows):
            raise ValueError("Rows must all have %d values" % n_columns)
        logging.info("Inserting %d rows into table %s", len(rows), table_name)

        # Note that sqlalchemy uses "execute" as a wrapper around
        # DB-API's "executemany"
        self.connection.execute(self.metadata.tables[table_name].insert(),
                                rows)

    def create(self, tables, overwrite, version):
        """Do the actual work of creating the collection, filling its tables with
        values, creating indices, and setting the datacache version metadata.

        Parameters
        ----------
        tables : list
            List of datacache.DatabaseTable objects

        overwrite : bool
            Overwrite existing tables?

        version : int
        """
        # The context manager commits at the close of the transaction
        with self.connection.begin() as trans:
            # If we're overwriting, get rid of any tables we're
            # about to create (including the metadata table).
            if overwrite:
                self.drop_tables([table.name for table in tables] +
                                 [self.metadata_table_name()])
            for table in tables:
                self._create_table(
                    table_name=table.name,
                    column_types=table.column_types,
                    primary=table.primary_key,
                    nullable=table.nullable)
                self._fill_table(table.name, table.rows)
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

        logging.info("Creating index on %s (%s)" % (
                table_name,
                ", ".join(index_columns)))
        index_name = "%s_index_%s" % (
            table_name,
            "_".join(index_columns))

        self.connection.execute(
            "CREATE INDEX %s ON %s (%s)" % (
                "\"%s\"" % index_name,
                "\"%s\"" % table_name,
                ", ".join(["\"%s\"" % col for col in index_columns])))

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
