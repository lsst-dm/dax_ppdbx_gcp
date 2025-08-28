# This file is part of dax_ppdbx_gcp
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
    project_id : `str`
        Google Cloud project ID.
    db_host : `str`
        Hostname of the database server.
    db_name : `str`
        Name of the database.
    db_user : `str`
        Username for database authentication.
    db_schema : `str`
        Schema name within the database.
    db_port : `int`, optional
        Port number for the database connection. Defaults to 5432.
    password_name : `str`, optional
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
        db_port: int = None,
        password_name: str | None = None,
    ):
        self._project_id = project_id
        self._db_host = db_host
        self._db_name = db_name
        self._db_user = db_user
        self._db_schema = db_schema
        if db_port is None:
            self._db_port = 5432
        self._engine: Engine | None = None
        self._table: Table | None = None
        self._password_name = password_name or "ppdb-db-password"

    @classmethod
    def from_env(cls) -> ReplicaChunkDatabase:
        """Create an instance using environment variables.

        Returns
        -------
        database : `ReplicaChunkDatabase`
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

        Does not currently accept an environment variable for the database
        port, so the default will be used (5432).
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
        db_url : `str`
            The database URL in the format
            ``postgresql://user:password@host:port/dbname``.
        schema_name : `str`
            The schema name to use within the database.
        project_id : `str`, optional
            The Google Cloud project ID. If not provided, it will be read from
            the environment.

        Returns
        -------
        database : `ReplicaChunkDatabase`
            An instance of `ReplicaChunkDatabase` initialized with the provided
            database URL and schema name.
        """
        engine = create_engine(db_url)
        if engine.dialect.name != "postgresql":
            raise ValueError("Database URL must be for a PostgreSQL database.")
        project_id = require_env("PROJECT_ID")
        if engine.url.host is None or engine.url.database is None or engine.url.username is None:
            raise ValueError("Database URL must include host, database name, and username.")
        return cls(
            project_id=project_id if project_id else require_env("PROJECT_ID"),
            db_host=engine.url.host,
            db_name=engine.url.database,
            db_user=engine.url.username,
            db_schema=schema_name,
        )

    @property
    def db_password(self) -> str:
        """Database password from Google Secret Manager (`str`, read-only).

        Notes
        -----
        This accesses the secret specified by `self._password_name` in Google
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
        """Database user (`str`, read-only)."""
        return self._db_user

    @property
    def db_host(self) -> str:
        """Database host (`str`, read-only)."""
        return self._db_host

    @property
    def db_name(self) -> str:
        """Database name (`str`, read-only)."""
        return self._db_name

    @property
    def db_schema(self) -> str:
        """Database schema name (`str`, read-only)."""
        return self._db_schema

    @property
    def db_port(self) -> int:
        """Database port number (`int`, read-only)."""
        return self._db_port

    @property
    def project_id(self) -> str:
        """Google Cloud project ID (`str`, read-only)."""
        return self._project_id

    @property
    def db_url(self) -> str:
        """Full database URL for SQLAlchemy, including the password (`str`,
        read-only).
        """
        return f"postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def db_url_safe(self) -> str:
        """Database URL without the password for logging or safe display
        (`str`, read-only).
        """
        return f"postgresql+psycopg2://{self.db_user}@{self.db_host}:5432/{self.db_name}"

    @property
    def engine(self) -> Engine:
        """SQLAlchemy engine for the database connection (`Engine`,
        read-only).
        """
        if self._engine is None:
            logging.info("Connecting to database at: %s (schema: %s)", self.db_url_safe, self.db_schema)
            self._engine = create_engine(
                self.db_url,
                pool_pre_ping=True,
            )
        return self._engine

    @property
    def table(self) -> Table:
        """SQLAlchemy `~sqlalchemy.Table` object for the PpdbReplicaChunk
        table.
        """
        if self._table is None:
            metadata = MetaData()
            self._table = Table("PpdbReplicaChunk", metadata, autoload_with=self.engine)
        return self._table

    @property
    def column_names(self) -> list[str]:
        """Column names of the PpdbReplicaChunk table (`list`[`str`],
        read-only).
        """
        return [col.name for col in self.table.columns]

    def execute(self, query: str, params: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
        """Execute a raw SQL query and return all results.

        Parameters
        ----------
        query : `str`
            The SQL query to execute.
        params : `dict`, optional
            Query parameters for parameterized SQL statements.

        Returns
        -------
        results : `list`[`tuple`[`Any`, ...]]
            A list of tuples containing the results of the query. Each tuple
            corresponds to a row in the result set.
        """
        logging.info("Executing query: %s", query)
        try:
            with self.engine.begin() as conn:
                result: Result = conn.execute(text(query), params or {})
                rows = result.fetchall()
                return [tuple(r) for r in rows]
        except Exception:
            logging.exception("Query failed: %s", query)
            raise

    def update(self, chunk_id: int, values: dict[str, Any]) -> int:
        """Update an existing replica chunk in the database.

        Parameters
        ----------
        chunk_id : `int`
            The ID of the replica chunk to update.
        values : `dict`[`str`, `Any`]
            A dictionary of column names and their new values to update.

        Returns
        -------
        count : `int`
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
        chunk_id : `int`
            The ID of the replica chunk to insert.
        values : `dict`[`str`, `Any`]
            A dictionary of column names and their values to insert.

        Returns
        -------
        count : `int`
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

    def get_promotable_chunks(self) -> list[int]:
        """
        Return the first uninterrupted sequence of staged chunks such that all
        prior chunks are promoted.

        Returns
        -------
        chunk_ids : `list`[`int`]
            A list of tuples containing the `apdb_replica_chunk` values of the
            promotable chunks.

        Notes
        -----
        This query finds the contiguous sequence of ``staged`` chunks beginning
        with the earliest chunk that is not yet ``promoted``, and ending just
        before the first chunk that is not ``staged``. If no such ending
        exists, all `staged` chunks from that point onward are returned. If no
        chunks are `staged` after the first non-`promoted` chunk, an empty list
        is returned.
        """
        query = f"""
        WITH start AS (
        SELECT MIN(apdb_replica_chunk) AS s
        FROM {self.table.name}
        WHERE status <> 'promoted'
        ),
        stop AS (
        SELECT MIN(p.apdb_replica_chunk) AS e
        FROM {self.table.name} p
        JOIN start ON TRUE
        WHERE start.s IS NOT NULL
            AND p.apdb_replica_chunk >= start.s
            AND p.status <> 'staged'
        )
        SELECT p.apdb_replica_chunk
        FROM {self.table.name} p
        JOIN start ON TRUE
        LEFT JOIN stop ON TRUE
        WHERE start.s IS NOT NULL
        AND p.status = 'staged'
        AND p.apdb_replica_chunk >= start.s
        AND (stop.e IS NULL OR p.apdb_replica_chunk < stop.e)
        ORDER BY p.apdb_replica_chunk;
        """
        rows = self.execute(query)
        return [r[0] for r in rows]

    def mark_chunks_promoted(self, promotable_chunks: list[int]) -> int:
        """Set status='promoted' for the given chunk IDs. Returns number
        updated. Uses ``_db.execute(...)`` by wrapping UPDATE in a CTE that
        returns a count.

        Parameters
        ----------
        promotable_chunks : `list`[`tuple`[`int`]]
            List of tuples containing the ``apdb_replica_chunk`` values of the
            promotable chunks. Each tuple should contain a single integer
            value.

        Returns
        -------
        count: `int`
            The number of rows updated in the database, which should be equal
            to the number of promotable chunks provided, if they were all found
            and updated successfully.
        """
        stmt = (
            update(self.table)
            .where(self.table.c.apdb_replica_chunk.in_(promotable_chunks),
                   self.table.c.status != "promoted")
            .values(status="promoted")
        )

        with self.engine.begin() as conn:
            result: Result = conn.execute(stmt)
            return result.rowcount
