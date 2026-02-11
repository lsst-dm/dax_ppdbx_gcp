import os
from abc import ABC, abstractmethod
from enum import StrEnum

from pydantic import BaseModel


class Status(StrEnum):
    """Enumeration of possible health check statuses."""

    OK = "OK"
    WARNING = "WARNING"
    DEGRADED = "DEGRADED"
    ERROR = "ERROR"
    UNKNOWN = "UNKNOWN"


class Result(BaseModel):
    """Result of a health check."""

    status: Status
    message: str


class HealthCheck(ABC):
    """Perform a health check on a PPDB configuration aspect or system."""

    @abstractmethod
    def check(self) -> Result:
        """Perform the health check."""
        ...


class ConfigHealthCheck(HealthCheck):
    """Health check for PPDB configuration."""

    def check(self) -> Result:
        ppdb_config_uri = os.environ.get("PPDB_CONFIG_URI")
        if not ppdb_config_uri:
            return Result(
                status=Status.ERROR,
                message="PPDB_CONFIG_URI environment variable is not set.",
            )

        # Placeholder implementation
        return Result(status=Status.OK, message="Configuration is valid.")
