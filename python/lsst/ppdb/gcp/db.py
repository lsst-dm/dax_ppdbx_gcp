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
from sqlalchemy import create_engine, insert, update, MetaData, Table
from sqlalchemy.engine import Engine

from .env import require_env


class ReplicaChunkDatabase:
    """Class to manage database operations for PPDB replica chunk metadata
    stored in a Postgres database."""

    def __init__(
        self,
        project_id: str,
        db_host: str,
        db_name: str,
        db_user: str,
        db_schema: str,
        password_name: str = "ppdb-db-password",
    ):
        self._project_id = project_id
        self._db_host = db_host
        self._db_name = db_name
        self._db_user = db_user
        self._db_schema = db_schema
        self._engine: Engine | None = None
        self._table: Table | None = None
        self._password_name = password_name

    @classmethod
    def from_env(cls) -> ReplicaChunkDatabase:
        """Create an instance using environment variables."""
        return ReplicaChunkDatabase(
            project_id=require_env("PROJECT_ID"),
            db_host=require_env("DB_HOST"),
            db_name=require_env("DB_NAME"),
            db_user=require_env("DB_USER"),
            db_schema=require_env("DB_SCHEMA"),
        )

    @property
    def db_password(self) -> str:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{self._project_id}/secrets/{self._password_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")

    @property
    def db_user(self) -> str:
        return self._db_user

    @property
    def db_host(self) -> str:
        return self._db_host

    @property
    def db_name(self) -> str:
        return self._db_name

    @property
    def db_schema(self) -> str:
        return self._db_schema

    @property
    def project_id(self) -> str:
        return self._project_id

    @property
    def db_url(self) -> str:
        return f"postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:5432/{self.db_name}"

    @property
    def db_url_safe(self) -> str:
        return f"postgresql+psycopg2://{self.db_user}@{self.db_host}:5432/{self.db_name}"

    @property
    def engine(self) -> Engine:
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
        if self._table is None:
            metadata = MetaData()
            self._table = Table("PpdbReplicaChunk", metadata, autoload_with=self.engine)
        return self._table

    def update(self, chunk_id: int, values: dict[str, Any]) -> None:
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

    def insert(self, chunk_id: int, values: dict[str, Any]) -> None:
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
