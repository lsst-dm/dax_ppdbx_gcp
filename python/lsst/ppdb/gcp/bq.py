# This file is part of ppdb-cloud-functions.
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

import logging

from google.cloud import bigquery

class QueryRunner:
    """Class to run BigQuery queries with logging.

    Parameters
    ----------
    project_id : str
        Google Cloud project ID.
    dataset_id : str
        BigQuery dataset ID.
    """

    def __init__(self, project_id: str, dataset_id: str):
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._bq_client = bigquery.Client(project=project_id)
        self._dataset = self._bq_client.get_dataset(dataset_id)
        self._location = self._dataset.location

    @property
    def dataset(self) -> bigquery.DatasetReference:
        """Get the BigQuery dataset reference."""
        return self._dataset

    @classmethod
    def log_job(cls, job: bigquery.job.QueryJob, label: str, level: int = logging.DEBUG) -> None:
        logging.log(
            level,
            "BQ %s: job_id=%s location=%s state=%s bytes_processed=%s bytes_billed=%s slot_millis=%s dml_rows=%s reference_tables=%s",
            label,
            job.job_id,
            job.location,
            job.state,
            getattr(job, "total_bytes_processed", None),
            getattr(job, "total_bytes_billed", None),
            getattr(job, "slot_millis", None),
            getattr(job, "num_dml_affected_rows", None),
            getattr(job, "referenced_tables", None)
        )

    def run_job(self, label: str, sql: str, job_config: bigquery.QueryJobConfig | None = None) -> bigquery.job.QueryJob:
        """Run a BigQuery job with the given SQL and configuration."""
        job = self._bq_client.query(sql, job_config=job_config, location=self.dataset.location)
        job.result()  # Wait for the job to complete
        self.log_job(job, label)
        return job
