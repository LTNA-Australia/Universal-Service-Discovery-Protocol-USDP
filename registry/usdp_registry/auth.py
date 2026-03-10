"""Publisher auth helpers."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import NAMESPACE_URL, uuid5

from .errors import UnauthorizedError


@dataclass(slots=True, frozen=True)
class AuthorizedPublisher:
    publisher_id: str
    publisher_name: str
    identity_type: str = "bearer_token"
    role: str = "publisher"


class PublisherAuthorizer:
    def __init__(
        self,
        publisher_tokens: tuple[str, ...],
        admin_tokens: tuple[str, ...] = (),
        peer_tokens: tuple[str, ...] = (),
    ) -> None:
        self._publishers = {}
        for entry in publisher_tokens:
            publisher = self._build_publisher(entry, role="publisher")
            self._publishers[publisher["token"]] = AuthorizedPublisher(
                publisher_id=publisher["publisher_id"],
                publisher_name=publisher["publisher_name"],
                identity_type="bearer_token",
                role="publisher",
            )
        for entry in admin_tokens:
            admin = self._build_publisher(entry, role="admin")
            self._publishers[admin["token"]] = AuthorizedPublisher(
                publisher_id=admin["publisher_id"],
                publisher_name=admin["publisher_name"],
                identity_type="bearer_token",
                role="admin",
            )
        for entry in peer_tokens:
            peer = self._build_publisher(entry, role="peer")
            self._publishers[peer["token"]] = AuthorizedPublisher(
                publisher_id=peer["publisher_id"],
                publisher_name=peer["publisher_name"],
                identity_type="bearer_token",
                role="peer",
            )

    def authorize(self, authorization_header: str | None) -> AuthorizedPublisher:
        return self._authorize(authorization_header, allowed_roles={"publisher", "admin", "peer"})

    def authorize_publisher(self, authorization_header: str | None) -> AuthorizedPublisher:
        return self._authorize(authorization_header, allowed_roles={"publisher", "admin"})

    def authorize_admin(self, authorization_header: str | None) -> AuthorizedPublisher:
        return self._authorize(authorization_header, allowed_roles={"admin"})

    def authorize_peer(self, authorization_header: str | None) -> AuthorizedPublisher:
        return self._authorize(authorization_header, allowed_roles={"peer", "admin"})

    def _authorize(self, authorization_header: str | None, *, allowed_roles: set[str]) -> AuthorizedPublisher:
        if not self._publishers:
            raise UnauthorizedError("Publisher authentication is not configured.")

        if not authorization_header:
            raise UnauthorizedError()

        scheme, _, token = authorization_header.partition(" ")
        if scheme.lower() != "bearer" or not token:
            raise UnauthorizedError()

        publisher = self._publishers.get(token.strip())
        if publisher is None:
            raise UnauthorizedError()
        if publisher.role not in allowed_roles:
            raise UnauthorizedError()
        return publisher

    def _build_publisher(self, entry: str, *, role: str) -> dict:
        raw = entry.strip()
        publisher_name = ""
        token = raw
        if "=" in raw:
            publisher_name, token = [item.strip() for item in raw.split("=", 1)]

        publisher_id = str(uuid5(NAMESPACE_URL, f"{role}:{token}"))
        return {
            "token": token,
            "publisher_id": publisher_id,
            "publisher_name": publisher_name or f"{role}-{publisher_id[:8]}",
        }
