"""SDK exceptions."""


class SDKError(Exception):
    """Base SDK exception."""


class SDKHTTPError(SDKError):
    def __init__(self, status: int, body: object) -> None:
        super().__init__(f"Registry request failed with status {status}")
        self.status = status
        self.body = body


class SDKRequestError(SDKError):
    """Raised for malformed SDK usage."""
