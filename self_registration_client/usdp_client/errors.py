"""Client-side exceptions."""


class ClientError(Exception):
    """Base client exception."""


class ClientHTTPError(ClientError):
    def __init__(self, status: int, body: object) -> None:
        super().__init__(f"Registry request failed with status {status}")
        self.status = status
        self.body = body


class ClientRequestError(ClientError):
    """Raised for malformed local client usage."""
