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

import json
import logging

from google.api_core.exceptions import NotFound
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future

__all__ = ["Publisher"]


class Publisher:
    """Publisher class for Google Cloud Pub/Sub.

    Parameters
    ----------
    project_id : str
        The Google Cloud project ID where the Pub/Sub topic is located.
    topic_name : str
        The name of the Pub/Sub topic to which messages will be published.
    """

    def __init__(self, project_id: str, topic_name: str):
        """
        Initialize the Publisher with the project ID and topic name.
        """
        self.publisher = PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_name)

    def validate_topic_exists(self) -> None:
        """
        Validate that the Pub/Sub topic exists.
        """
        try:
            self.publisher.get_topic(request={"topic": self.topic_path})
        except NotFound:
            logging.exception("Pub/Sub topic does not exist: %s", self.topic_path)
            raise

    def publish(self, message: dict) -> Future:
        """
        Publish a message to the Pub/Sub topic specified during initialization.

        Parameters
        ----------
        message : dict
            The message to be published, which will be converted to JSON.
        """
        try:
            return self.publisher.publish(self.topic_path, json.dumps(message).encode("utf-8"))
        except Exception:
            logging.exception("Failed to publish message to topic %s: %s", self.topic_path, message)
            raise
