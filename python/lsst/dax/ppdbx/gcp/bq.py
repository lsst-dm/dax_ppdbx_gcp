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

__all__ = ["AnyBQJob", "NoPromotableChunksError", "QueryRunner", "ReplicaChunkPromoter"]

import logging
from collections.abc import Callable
from typing import TypeAlias

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from .env import require_env


class QueryRunner:
    """Class to run BigQuery queries with logging.

    Parameters
    ----------
    project_id : `str`
        Google Cloud project ID.
    dataset_id : `str`
        BigQuery dataset ID.
    """

    def __init__(self, project_id: str, dataset_id: str):
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._bq_client = bigquery.Client(project=project_id)
        self._dataset = self._bq_client.get_dataset(f"{project_id}.{dataset_id}")
        self._location = self._dataset.location

    @classmethod
    def from_env(cls) -> QueryRunner:
        """Create a QueryRunner instance using environment variables.

        Returns
        -------
        query_runner: `QueryRunner`
            An instance of QueryRunner initialized with project and dataset IDs
            from environment variables.
        """
        project_id = require_env("PROJECT_ID")
        dataset_id = require_env("DATASET_ID")
        return cls(project_id, dataset_id)

    @property
    def project_id(self) -> str:
        """Google Cloud project ID (`str`, read-only)."""
        return self._project_id

    @property
    def dataset(self) -> bigquery.Dataset:
        """Dataset reference (`bigquery.Dataset`, read-only)."""
        return self._dataset

    @property
    def dataset_id(self) -> str:
        """Dataset ID (`str`, read-only)."""
        return self._dataset_id

    @property
    def location(self) -> str:
        """Dataset location, typically the region where it is hosted (`str`,
        read-only).
        """
        return self._location

    @classmethod
    def log_job(cls, job: AnyBQJob, label: str, level: int = logging.DEBUG) -> None:
        """Log details of a BigQuery job.

        Parameters
        ----------
        job : `bigquery.job.QueryJob`
            The BigQuery job to log.
        label : `str`
            A label for the job, typically indicating the type of operation
            (e.g., "insert", "delete", "copy").
        level : `int`, optional
            The logging level to use for the log message. Defaults to
            `logging.DEBUG`.
        """
        logging.log(
            level,
            "BQ %s: job_id=%s location=%s state=%s bytes_processed=%s bytes_billed=%s slot_millis=%s "
            "dml_rows=%s reference_tables=%s",
            label,
            job.job_id,
            job.location,
            job.state,
            getattr(job, "total_bytes_processed", None),
            getattr(job, "total_bytes_billed", None),
            getattr(job, "slot_millis", None),
            getattr(job, "num_dml_affected_rows", None),
            getattr(job, "referenced_tables", None),
        )

    def run_job(
        self, label: str, sql: str, job_config: bigquery.QueryJobConfig | None = None
    ) -> bigquery.job.QueryJob:
        """Run a BigQuery job with the given SQL and configuration.

        Parameters
        ----------
        label : `str`
            A label for the job, typically indicating the type of operation
            (e.g., "insert", "delete", "copy").
        sql : `str`
            The SQL query to execute.
        job_config : `bigquery.QueryJobConfig`, optional
            Configuration for the job, such as query parameters or write
            dispositions. If not provided, a default configuration will be
            used.

        Returns
        -------
        job: `bigquery.job.QueryJob`
            The BigQuery job object representing the executed query. This can
            be used to check the status of the job, retrieve results, or log
            additional details.
        """
        job = self._bq_client.query(sql, job_config=job_config, location=self.dataset.location)
        job.result()  # Wait for the job to complete
        self.log_job(job, label)
        return job


class NoPromotableChunksError(Exception):
    """Exception raised when there are no promotable chunks available."""

    pass


AnyBQJob: TypeAlias = (
    bigquery.job.QueryJob
    | bigquery.job.LoadJob
    | bigquery.job.CopyJob
    | bigquery.job.ExtractJob
    | bigquery.job.UnknownJob
)


class ReplicaChunkPromoter:
    """Class to promote replica chunks in BigQuery.

    Parameters
    ----------
     promotable_chunks: `list`[`int`]
        Sequence of tuples containing the APDB replica chunk IDs to promote.
    runner : `QueryRunner`, optional
        An instance of `QueryRunner` to execute queries. If not provided, a new
        instance will be created using environment variables.
    table_names : `list`[`str`], optional
        List of table names to promote with standard default.
    """

    def __init__(
        self,
        promotable_chunks: list[int],
        runner: QueryRunner | None = None,
        table_names: list[str] | None = None,
    ):
        self._promotable_chunks = promotable_chunks
        self._runner = runner or QueryRunner.from_env()
        # DM-52326: Hard-coded table names; these should be passed in from
        # config.
        self._table_names = table_names or ["DiaObject", "DiaSource", "DiaForcedSource"]
        self._bq_client = bigquery.Client(project=self._runner.project_id)
        self._phases = {
            "build_tmp": self._copy_to_promoted_tmp,
            "promote_prod": self._promote_tmp_to_prod,
            "delete_staged_chunks": self._delete_staged_chunks,
            "cleanup": self._cleanup_promoted_tmp,
        }

    @property
    def project_id(self) -> str:
        """Google Cloud project ID (`str`, read-only)."""
        return self._runner.project_id

    @property
    def dataset_id(self) -> str:
        """Dataset ID (`str`, read-only)."""
        return self._runner.dataset_id

    @property
    def table_names(self) -> list[str]:
        """List of table names to promote (`list`[`str`], read-only)."""
        return self._table_names

    @property
    def promotable_chunks(self) -> list[int]:
        """List of promotable chunks (`list[`int`],
        read-only).
        """
        return self._promotable_chunks

    @promotable_chunks.setter
    def promotable_chunks(self, chunks: list[int]) -> None:
        if not chunks:
            raise NoPromotableChunksError("No promotable chunks provided")
        self._promotable_chunks = chunks

    @property
    def runner(self) -> QueryRunner:
        """Runner for executing BigQuery jobs (`QueryRunner`, read-only)."""
        return self._runner

    @property
    def bq_client(self) -> bigquery.Client:
        """Client for interacting with BigQuery (`bigquery.Client`,
        read-only).
        """
        return self._bq_client

    @property
    def phases(self) -> dict[str, Callable]:
        """Phases of the promotion process as a dictionary mapping phase names
        to their corresponding class methods (`dict`[`str`, `Callable`],
        read-only).
        """
        return self._phases

    @property
    def table_prod_refs(self) -> list[str]:
        """Fully-qualified production table references (`list`[`str`],
        read-only).
        """
        return [f"{self.project_id}.{self.dataset_id}.{table_name}" for table_name in self.table_names]

    @property
    def table_staging_refs(self) -> list[str]:
        """Fully-qualified staging table references (`list`[`str`],
        read-only).
        """
        return [
            f"{self.project_id}.{self.dataset_id}._{table_name}_staging" for table_name in self.table_names
        ]

    @property
    def table_promoted_tmp_refs(self) -> list[str]:
        """Fully-qualified promoted temporary table references (`list`[`str`],
        read-only).
        """
        return [
            f"{self.project_id}.{self.dataset_id}._{table_name}_promoted_tmp"
            for table_name in self.table_names
        ]

    def _execute_phase(self, phase: str) -> None:
        """Execute a specific promotion phase.

        Parameters
        ----------
        phase : `str`
            The name of the promotion phase to execute. This should be one of
            the keys in the `phases` property.
        """
        if phase not in self.phases:
            raise ValueError(f"Unknown promotion phase: {phase}")
        logging.debug("Executing promotion phase: %s", phase)
        self._phases[phase]()

    def _copy_to_promoted_tmp(self) -> None:
        """
        Build ``_{table_name}_promoted_tmp`` efficiently by cloning prod and
        inserting only staged rows for the given replica chunk IDs.
        """
        job_cfg = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", self.promotable_chunks)]
        )

        for prod_ref, tmp_ref, stage_ref in zip(
            self.table_prod_refs, self.table_promoted_tmp_refs, self.table_staging_refs
        ):
            # Drop any existing tmp table (should not exist but just to be
            # safe)
            self.runner.run_job("drop_tmp", f"DROP TABLE IF EXISTS `{tmp_ref}`")

            # Clone prod table structure and data (zero-copy)
            self.runner.run_job("clone_prod", f"CREATE TABLE `{tmp_ref}` CLONE `{prod_ref}`")

            # Insert staged rows into tmp, excluding apdb_replica_chunk column
            sql = f"""
            INSERT INTO `{tmp_ref}`
            SELECT * EXCEPT(apdb_replica_chunk)
            FROM `{stage_ref}`
            WHERE apdb_replica_chunk IN UNNEST(@ids)
            """
            self.runner.run_job("insert_staged_to_tmp", sql, job_config=job_cfg)

    def _promote_tmp_to_prod(self) -> None:
        """
        Swap each prod table with its corresponding *_promoted_tmp by replacing
        prod contents in a single atomic copy job. This preserves schema,
        partitioning, and clustering with zero-copy when in the same dataset.
        """
        for prod_ref, tmp_ref in zip(self.table_prod_refs, self.table_promoted_tmp_refs):
            # Ensure tmp exists
            try:
                self.bq_client.get_table(tmp_ref)
            except NotFound as e:
                raise RuntimeError(f"Missing tmp table for promotion: {tmp_ref}") from e

            # Atomic zero-copy replacement of prod with tmp
            copy_cfg = bigquery.CopyJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            job = self.bq_client.copy_table(
                tmp_ref, prod_ref, job_config=copy_cfg, location=self._runner.location
            )
            job.result()
            QueryRunner.log_job(job, "promote_tmp_to_prod")

    def _cleanup_promoted_tmp(self) -> None:
        """Drop the promotion temporary tables."""
        for tmp_ref in self.table_promoted_tmp_refs:
            self.bq_client.delete_table(tmp_ref, not_found_ok=True)
            logging.debug("Dropped %s (if it existed)", tmp_ref)

    def _delete_staged_chunks(self) -> None:
        """Delete only rows for the promoted replica chunk IDs from each
        staging table.
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", self.promotable_chunks)]
        )

        for staging_ref in self.table_staging_refs:
            try:
                sql = f"DELETE FROM `{staging_ref}` WHERE apdb_replica_chunk IN UNNEST(@ids)"
                self.runner.run_job("delete_staged_chunks", sql, job_config=job_config)
                logging.debug(
                    "Deleted %d chunk(s) from staging table %s", len(self.promotable_chunks), staging_ref
                )
            except NotFound:
                logging.warning("Staging table %s does not exist, skipping delete", staging_ref)

    def promote_chunks(self) -> None:
        """Promote APDB replica chunks into production."""
        try:
            for phase in ("build_tmp", "promote_prod", "delete_staged_chunks"):
                self._execute_phase(phase)
        finally:
            try:
                self._execute_phase("cleanup")
            except Exception:
                logging.exception("Cleanup of temporary tables failed")
