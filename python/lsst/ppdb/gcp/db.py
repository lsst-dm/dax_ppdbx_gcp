# This file is part of ppdb-gcp
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ["ReplicaChunkDatabase"]


import logging
from typing import Any

from google.cloud import secretmanager
from sqlalchemy import MetaData, Table, create_engine, insert, text, update
from sqlalchemy.engine import Engine, Result

from .env import require_env


class ReplicaChunkDatabase:
    """Class to manage database operations for PPDB replica chunk metadata
    stored in a Postgres database.

    Parameters
    ----------
    project_id : str
        Google Cloud project ID.
    db_host : str
        Hostname of the database server.
    db_name : str
        Name of the database.
    db_user : str
        Username for database authentication.
    db_schema : str
        Schema name within the database.
    password_name : str, optional
        Name of the secret in Google Secret Manager that contains the database
        password. Defaults to "ppdb-db-password".
    """

    def __init__(
        self,
        project_id: str,
        db_host: str,
        db_name: str,
        db_user: str,
        db_schema: str,
        password_name: str | None = None,
    ):
        self._project_id = project_id
        self._db_host = db_host
        self._db_name = db_name
        self._db_user = db_user
        self._db_schema = db_schema
        self._engine: Engine | None = None
        self._table: Table | None = None
        self._password_name = password_name or "ppdb-db-password"

    @classmethod
    def from_env(cls) -> ReplicaChunkDatabase:
        """Create an instance using environment variables.

        Returns
        -------
        ReplicaChunkDatabase
            An instance of `ReplicaChunkDatabase` initialized with values from
            environment variables.

        Notes
        -----
        The environment variables should be set as follows:
        - ``PROJECT_ID``: Google Cloud project ID.
        - ``DB_HOST``: Hostname of the database server.
        - ``DB_NAME``: Name of the database.
        - ``DB_USER``: Username for database authentication.
        - ``DB_SCHEMA``: Schema name within the database.
        - ``DB_PASSWORD_NAME``: (optional) Name of the secret in Google Secret
          Manager that contains the database password. Defaults to
          "ppdb-db-password".
        """
        return cls(
            project_id=require_env("PROJECT_ID"),
            db_host=require_env("DB_HOST"),
            db_name=require_env("DB_NAME"),
            db_user=require_env("DB_USER"),
            db_schema=require_env("DB_SCHEMA"),
        )

    @classmethod
    def from_url(cls, db_url: str, schema_name: str, project_id: str | None = None) -> ReplicaChunkDatabase:
        """Create an instance using a database URL and a schema name, plus an
        optional project ID. The project ID will be read from the environment
        if not provided.

        Parameters
        ----------
        db_url : str
            The database URL in the format
            ``postgresql://user:password@host:port/dbname``.
        schema_name : str
            The schema name to use within the database.
        project_id : str, optional
            The Google Cloud project ID. If not provided, it will be read from
            the environment.

        Returns
        -------
        ReplicaChunkDatabase
            An instance of `ReplicaChunkDatabase` initialized with the provided
            database URL and schema name.
        """
        engine = create_engine(db_url)
        if engine.dialect.name != "postgresql":
            raise ValueError("Database URL must be for a PostgreSQL database.")
        project_id = require_env("PROJECT_ID")
        return cls(
            project_id=project_id if project_id else require_env("PROJECT_ID"),
            db_host=engine.url.host,
            db_name=engine.url.database,
            db_user=engine.url.username,
            db_schema=schema_name,
        )

    @property
    def db_password(self) -> str:
        """Retrieve the database password from Google Secret Manager.

        Returns
        -------
        str
            The database password retrieved from the secret manager.

        Notes
        -----
        This method accesses the secret named `self._password_name` in Google
        Secret Manager and returns its value as a string. It raises an
        exception if the secret cannot be accessed or if the secret is not
        found. This password should not be printed or logged in order to avoid
        security risks.
        """
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{self._project_id}/secrets/{self._password_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")

    @property
    def db_user(self) -> str:
        """Return the database user.

        Returns
        -------
        str
            The database user name used for authentication.
        """
        return self._db_user

    @property
    def db_host(self) -> str:
        """Return the database host.

        Returns
        -------
        str
            The hostname of the database server.
        """
        return self._db_host

    @property
    def db_name(self) -> str:
        """Return the database name.

        Returns
        -------
        str
            The name of the database used for tracking replica chunks.
        """
        return self._db_name

    @property
    def db_schema(self) -> str:
        """Return the database schema.

        Returns
        -------
        str
            The schema name within the database containing the
            ``PpdbReplicaChunk`` table.
        """
        return self._db_schema

    @property
    def project_id(self) -> str:
        """Return the Google Cloud project ID.

        Returns
        -------
        str
            The Google Cloud project ID associated with this database.
        """
        return self._project_id

    @property
    def db_url(self) -> str:
        """Return the database URL for SQLAlchemy.

        Returns
        -------
        str
            The database URL in the format::
                postgresql+psycopg2://user:password@host:port/dbname
                ?options=-c%20search_path=schema_name
        """
        return f"postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:5432/{self.db_name}"

    @property
    def db_url_safe(self) -> str:
        """Return a database URL without the password for logging or safe
        display.

        Returns
        -------
        str
            The database URL with the password omitted, in the format::
                postgresql+psycopg2://user@host:port/dbname
                ?options=-c%20search_path=schema_name
        """
        return f"postgresql+psycopg2://{self.db_user}@{self.db_host}:5432/{self.db_name}"

    @property
    def engine(self) -> Engine:
        """Return the SQLAlchemy engine for the database connection.

        Returns
        -------
        Engine
            The SQLAlchemy engine used to connect to the database.
        """
        if self._engine is None:
            logging.info("Connecting to database at: %s (schema: %s)", self.db_url_safe, self.db_schema)
            self._engine = create_engine(
                self.db_url,
                pool_pre_ping=True,
                connect_args={"options": f"-c search_path={self.db_schema}"},
            )
        return self._engine

    @property
    def table(self) -> Table:
        """Return the SQLAlchemy Table object for the PpdbReplicaChunk
        table.

        Returns
        -------
        Table
            The SQLAlchemy Table object representing the
            ``PpdbReplicaChunk`` table in the database.
        """
        if self._table is None:
            metadata = MetaData()
            self._table = Table("PpdbReplicaChunk", metadata, autoload_with=self.engine)
        return self._table

    @property
    def column_names(self) -> list[str]:
        """Return the column names of the PpdbReplicaChunk table.

        Returns
        -------
        list[str]
            A list of column names in the `PpdbReplicaChunk` table.
        """
        return [col.name for col in self.table.columns]

    def execute(self, query: str, params: dict[str, Any] | None = None) -> list[tuple]:
        """Execute a raw SQL query and return all results.

        Parameters
        ----------
        query : str
            The SQL query to execute.
        params : dict, optional
            Query parameters for parameterized SQL statements.

        Returns
        -------
        list[tuple]
            Query results as a list of row tuples.
        """
        logging.info("Executing query: %s", query)
        try:
            with self.engine.begin() as conn:
                result: Result = conn.execute(text(query), params or {})
                return result.fetchall()
        except Exception:
            logging.exception("Query failed: %s", query)
            raise

    def update(self, chunk_id: int, values: dict[str, Any]) -> int:
        """Update an existing replica chunk in the database.

        Parameters
        ----------
        chunk_id : int
            The ID of the replica chunk to update.
        values : dict[str, Any]
            A dictionary of column names and their new values to update.

        Returns
        -------
        int
            The number of rows updated. This should be 1 if the update is
            successful, or 0 if no rows were updated (e.g., if the chunk ID
            does not exist or the status is already set to the new value).
        """
        logging.info("Preparing to update replica chunk %d with values: %s", chunk_id, values)
        stmt = update(self.table).where(self.table.c.apdb_replica_chunk == chunk_id).values(values)
        with self.engine.begin() as conn:
            result = conn.execute(stmt)
            affected_rows = result.rowcount

        new_status = values.get("status")
        if affected_rows == 0:
            logging.warning(
                "No rows updated for replica chunk %s with status '%s'",
                chunk_id,
                new_status,
            )
        else:
            logging.info(
                "Successfully updated %d row(s) for replica chunk %s to status '%s'",
                affected_rows,
                chunk_id,
                new_status,
            )
        return affected_rows

    def insert(self, chunk_id: int, values: dict[str, Any]) -> int:
        """Insert a new replica chunk into the database.

        Parameters
        ----------
        chunk_id : int
            The ID of the replica chunk to insert.
        values : dict[str, Any]
            A dictionary of column names and their values to insert.

        Returns
        -------
        int
            The number of rows inserted, which should be 1 if successful.
        """
        insert_values = {"apdb_replica_chunk": chunk_id, **values}
        logging.info("Preparing to insert replica chunk %d with values: %s", chunk_id, insert_values)
        stmt = insert(self.table).values(insert_values)
        with self.engine.begin() as conn:
            result = conn.execute(stmt)
            affected_rows = result.rowcount

        new_status = insert_values.get("status")
        if affected_rows == 0:
            logging.error(
                "Expected to insert replica chunk %s with status '%s', but no rows were inserted.",
                chunk_id,
                new_status,
            )
            raise RuntimeError(
                f"No rows inserted for apdb_replica_chunk={chunk_id} - insert silently failed."
            )
        else:
            logging.info(
                "Successfully inserted %d row(s) for replica chunk %s with status '%s'",
                affected_rows,
                chunk_id,
                new_status,
            )
        return affected_rows

    def get_promotable_chunks(self) -> list[tuple[int]]:
        """
        Return the first uninterrupted sequence of staged chunks such that all
        prior chunks are promoted.

        Returns
        -------
        list[tuple[int]]
            A list of tuples containing the `apdb_replica_chunk` values of the
            promotable chunks.
        """
        query = """
        WITH start AS (
        SELECT MIN(apdb_replica_chunk) AS s
        FROM "PpdbReplicaChunk"
        WHERE status <> 'promoted'
        ),
        stop AS (
        SELECT MIN(p.apdb_replica_chunk) AS e
        FROM "PpdbReplicaChunk" p
        JOIN start ON TRUE
        WHERE start.s IS NOT NULL
            AND p.apdb_replica_chunk >= start.s
            AND p.status <> 'staged'
        )
        SELECT p.apdb_replica_chunk
        FROM "PpdbReplicaChunk" p
        JOIN start ON TRUE
        LEFT JOIN stop ON TRUE
        WHERE start.s IS NOT NULL
        AND p.status = 'staged'
        AND p.apdb_replica_chunk >= start.s
        AND (stop.e IS NULL OR p.apdb_replica_chunk < stop.e)
        ORDER BY p.apdb_replica_chunk;
        """
        return self.execute(query)

    def mark_chunks_promoted(self, promotable_chunks: list[tuple[int]]) -> int:
        """Set status='promoted' for the given chunk IDs. Returns number
        updated. Uses ``_db.execute(...)`` by wrapping UPDATE in a CTE that
        returns a count.

        Parameters
        ----------
        promotable_chunks : list[tuple[int]]
            List of tuples containing the `apdb_replica_chunk` values of the
            promotable chunks. Each tuple should contain a single integer
            value.

        Returns
        -------
        int
            The number of rows updated in the database, which should be equal
            to the number of promotable chunks provided, if they were all found
            and updated successfully.
        """
        ids = [int(row[0]) for row in promotable_chunks if row and row[0] is not None]
        if not ids:
            return 0

        sql = """
        WITH updated AS (
        UPDATE "PpdbReplicaChunk"
        SET status = 'promoted'
        WHERE apdb_replica_chunk = ANY(:ids)
        AND status <> 'promoted'
        RETURNING apdb_replica_chunk
        )
        SELECT COUNT(*) FROM updated;
        """
        rows = self.execute(sql, {"ids": ids})
        return int(rows[0][0]) if rows else 0
