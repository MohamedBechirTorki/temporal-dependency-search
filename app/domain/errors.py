"""Domain exceptions. Mapped to HTTP responses in app.api.errors."""
from __future__ import annotations

from typing import Any


class DomainError(Exception):
    status_code = 400
    code = "domain_error"

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class NotFoundError(DomainError):
    status_code = 404
    code = "not_found"


class ConflictError(DomainError):
    status_code = 409
    code = "conflict"


class DuplicateDependencyError(ConflictError):
    code = "duplicate_dependency"


class CycleDetectedError(ConflictError):
    code = "cycle_detected"


class InvalidRequestError(DomainError):
    status_code = 422
    code = "invalid_request"


class StorageUnavailableError(DomainError):
    status_code = 503
    code = "storage_unavailable"
