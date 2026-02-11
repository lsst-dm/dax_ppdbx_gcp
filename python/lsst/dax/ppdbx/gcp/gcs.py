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

__all__ = ["DeleteError", "StorageClient", "UploadError", "check_bucket_exists"]

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud.storage import Client


class StorageError(RuntimeError):
    """Base error for this module."""


class UploadError(RuntimeError):
    """Single-file upload failure."""


class DeleteError(StorageError):
    """Error raised for a failed recursive delete.

    Parameters
    ----------
    prefix : `str`
        GCS prefix that was being deleted.
    """

    def __init__(self, prefix: str) -> None:
        self.prefix: str = prefix
        super().__init__(f"delete failed under prefix: {self.prefix}")


def check_bucket_exists(bucket_name: str) -> None:
    """Check if a Google Cloud Storage bucket exists.

    Parameters
    ----------
    bucket_name : `str`
        Name of the bucket to check.

    Raises
    ------
    google.cloud.exceptions.NotFound
        If the bucket does not exist.
    """
    try:
        Client().get_bucket(bucket_name)
    except NotFound as e:
        raise LookupError(f"Bucket {bucket_name} not found") from e


class StorageClient:
    """A client for interacting with Google Cloud Storage.

    Parameters
    ----------
    bucket_name : `str`
        The name of the Google Cloud Storage bucket to interact with.

    Notes
    -----
    This class provides methods to interact with a specified Google Cloud
    Storage bucket using the ``google.cloud.storage.Client`` class.
    """

    def __init__(self, bucket_name: str):
        self.client = Client()
        self.bucket_name = bucket_name
        self.bucket = self.client.bucket(bucket_name)

    def create_bucket(self, location: str = "US") -> None:
        """Create the bucket if it doesn't already exist.

        Parameters
        ----------
        location : `str`, optional
            The location for the bucket (e.g., "US", "us-central1").
            Defaults to "US" for multi-region.

        Raises
        ------
        google.cloud.exceptions.Conflict
            If the bucket already exists.
        google.cloud.exceptions.BadRequest
            If the bucket name is invalid.
        """
        try:
            self.bucket = self.client.create_bucket(self.bucket_name, location=location)
        except Exception as e:
            raise StorageError(f"Failed to create bucket {self.bucket_name}") from e

    def delete_bucket(self, force: bool = False) -> None:
        """Delete the bucket.

        Parameters
        ----------
        force : `bool`, optional
            If True, delete all objects in the bucket before deleting the
            bucket.
            Defaults to False.

        Raises
        ------
        google.cloud.exceptions.Conflict
            If the bucket is not empty and force is False.
        """
        try:
            if force:
                # Delete all objects in the bucket first
                blobs = self.bucket.list_blobs()
                for blob in blobs:
                    blob.delete()
            self.bucket.delete()
        except Exception as e:
            raise StorageError(f"Failed to delete bucket {self.bucket_name}") from e

    def check_bucket_exists(self) -> bool:
        """Check if the bucket exists.

        Returns
        -------
        bool
            True if the bucket exists, False otherwise.
        """
        try:
            self.client.get_bucket(self.bucket_name)
            return True
        except NotFound:
            return False

    def upload_file(self, blob_name: str, file_path: Path) -> None:
        """Upload a file to the specified blob name in the bucket.

        Parameters
        ----------
        blob_name : `str`
            The name of the blob in the bucket.
        file_path : `pathlib.Path`
            The local path to the file to upload.
        """
        try:
            blob = self.bucket.blob(blob_name)
            blob.upload_from_filename(file_path)
        except Exception as e:
            raise UploadError(f"upload failed: {str(file_path)} -> {blob_name}") from e

    def upload_files(self, gcs_names: dict[Path, str]) -> None:
        """Upload files in parallel.

        Parameters
        ----------
        gcs_names : `dict`[`Path`, `str`]
            A dictionary mapping local file paths (as `Path` objects) to their
            corresponding GCS object names (as strings).

        Raises
        ------
        ExceptionGroup
            Raised if any uploads fail, an `ExceptionGroup` is raised
            containing `UploadError` instances for each failed upload.
        """
        failures: list[UploadError] = []

        with ThreadPoolExecutor() as executor:
            # Execute upload for each pair of source path and GCS object name.
            # Map the futures to their corresponding source paths and object
            # names to handle exceptions later.
            future_to_upload = {
                executor.submit(self.upload_file, object_name, src_path): (src_path, object_name)
                for src_path, object_name in gcs_names.items()
            }

            for future in as_completed(future_to_upload):
                src_path, object_name = future_to_upload[future]
                try:
                    # Trigger exception for the upload, if any.
                    future.result()
                except Exception as e:
                    # Capture the exception and create an UploadError.
                    upload_error = UploadError(f"upload failed: {str(src_path)} -> {object_name}")

                    upload_error.__cause__ = e  # Preserve original exception
                    failures.append(upload_error)

        if failures:
            raise ExceptionGroup("One or more uploads failed.", failures)

    def upload_from_string(self, blob_name: str, file_content: str) -> None:
        """Upload a string as a blob to the specified bucket.

        Parameters
        ----------
        blob_name : `str`
            The name of the blob in the bucket.
        file_content : `str`
            The content to upload as a string.
        """
        try:
            blob = self.bucket.blob(blob_name)
            blob.upload_from_string(file_content)
        except Exception as e:
            raise UploadError(f"upload failed: <string> -> {blob_name}") from e

    def delete_recursive(self, gcs_prefix: str) -> None:
        """Recursively delete all of the objects under a GCS prefix.

        Parameters
        ----------
        gcs_prefix : `str`
            The name of the blob to delete.
        """
        try:
            blobs = self.bucket.list_blobs(prefix=gcs_prefix)
            for blob in blobs:
                blob.delete()
        except Exception as e:
            raise DeleteError(gcs_prefix) from e
