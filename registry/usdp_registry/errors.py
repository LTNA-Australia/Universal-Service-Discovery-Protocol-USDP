"""Shared registry exceptions."""


class RegistryError(Exception):
    """Base error type for API-safe failures."""

    def __init__(self, status: int, code: str, message: str, details: object | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details


class InvalidRequestError(RegistryError):
    def __init__(self, message: str, details: object | None = None) -> None:
        super().__init__(400, "INVALID_REQUEST", message, details)


class ValidationError(RegistryError):
    def __init__(self, message: str, details: object | None = None) -> None:
        super().__init__(400, "VALIDATION_ERROR", message, details)


class UnauthorizedError(RegistryError):
    def __init__(self, message: str = "Missing or invalid publisher token.", details: object | None = None) -> None:
        super().__init__(401, "UNAUTHORIZED", message, details)


class ForbiddenError(RegistryError):
    def __init__(self, message: str = "Publisher is not allowed to modify this service.", details: object | None = None) -> None:
        super().__init__(403, "FORBIDDEN", message, details)


class NotFoundError(RegistryError):
    def __init__(self, message: str, details: object | None = None) -> None:
        super().__init__(404, "NOT_FOUND", message, details)


class ConflictError(RegistryError):
    def __init__(self, message: str, details: object | None = None) -> None:
        super().__init__(409, "CONFLICT", message, details)


class PayloadTooLargeError(RegistryError):
    def __init__(self, message: str = "Request body exceeds the configured size limit.", details: object | None = None) -> None:
        super().__init__(413, "PAYLOAD_TOO_LARGE", message, details)


class RateLimitedError(RegistryError):
    def __init__(self, message: str = "Request rate limit exceeded.", details: object | None = None) -> None:
        super().__init__(429, "RATE_LIMITED", message, details)
