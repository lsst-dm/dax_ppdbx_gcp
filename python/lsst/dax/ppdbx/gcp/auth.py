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

__all__ = ["get_auth_default"]


import google.auth
from google.auth.credentials import Credentials


def get_auth_default() -> tuple[Credentials, str]:
    """Set up Google authentication for the application.

    This function checks for the presence of the GOOGLE_APPLICATION_CREDENTIALS
    environment variable, which should point to a valid service account key
    file. It then initializes the Google Cloud credentials and project ID for
    use in the application and returns them. The project ID is determined from
    the credentials, and an error is raised if it cannot be determined.

    Returns
    -------
    credentials : `Credentials`
        Google Cloud credentials.
    project_id : `str`
        Project ID.
    """
    # Setup Google authentication
    try:
        credentials, project_id = google.auth.default()
        if not project_id:
            raise RuntimeError("Project ID could not be determined from the credentials.")
    except Exception as e:
        raise RuntimeError("Failed to setup Google credentials.") from e

    return credentials, project_id
