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

from google.cloud import resourcemanager_v3


def get_project_label(project_id: str, label_name: str) -> str | None:
    """
    Retrieve a specific label from a Google Cloud project.

    Parameters
    ----------
    project_id : str
        The GCP project ID.
    label_name : str
        The name of the label to retrieve.

    Returns
    -------
    label_value : str | None
        The value of the specified label if present, otherwise None.

    Notes
    -----
    This uses the Cloud Resource Manager v3 API.
    Application Default Credentials (ADC) must be configured, e.g. via
    `gcloud auth application-default login` or a service account in a GCP
    runtime.
    """
    client: resourcemanager_v3.ProjectsClient = resourcemanager_v3.ProjectsClient()

    project_name: str = f"projects/{project_id}"
    project: resourcemanager_v3.types.Project = client.get_project(name=project_name)

    labels: dict[str, str] = dict(project.labels)
    return labels.get(label_name)


def get_project_environment(project_id: str) -> str | None:
    """
    Retrieve the `environment` label from a Google Cloud project.

    Parameters
    ----------
    project_id : str
        The GCP project ID.

    Returns
    -------
    label_value : str | None
        The value of the `environment` label if present, otherwise None.

    Notes
    -----
    This uses the Cloud Resource Manager v3 API.
    Application Default Credentials (ADC) must be configured, e.g. via
    `gcloud auth application-default login` or a service account in a GCP
    runtime.
    """
    return get_project_label(project_id, "environment")
