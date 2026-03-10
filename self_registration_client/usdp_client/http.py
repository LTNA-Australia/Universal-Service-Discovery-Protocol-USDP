"""Low-level HTTP transport for the registration client."""

from __future__ import annotations

import json
import random
import time
from urllib import error, request

from .config import ClientConfig
from .errors import ClientHTTPError


def request_json(
    config: ClientConfig,
    method: str,
    path: str,
    payload: dict | None,
    *,
    extra_headers: dict[str, str] | None = None,
) -> dict:
    body = None
    headers = {
        "Authorization": f"Bearer {config.publisher_token}",
        "Accept": "application/json",
    }
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if extra_headers:
        headers.update(extra_headers)

    url = config.registry_url.rstrip("/") + path
    last_error: Exception | None = None

    for attempt in range(config.retry_attempts):
        req = request.Request(url, data=body, headers=headers, method=method)
        try:
            with request.urlopen(req, timeout=config.timeout_seconds) as response:
                response_body = response.read().decode("utf-8")
                return json.loads(response_body) if response_body else {}
        except error.HTTPError as exc:
            response_body = exc.read().decode("utf-8")
            exc.close()
            parsed = json.loads(response_body) if response_body else {}
            if 500 <= exc.code < 600 and attempt + 1 < config.retry_attempts:
                _sleep_before_retry(config, attempt)
                last_error = ClientHTTPError(exc.code, parsed)
                continue
            raise ClientHTTPError(exc.code, parsed) from exc
        except error.URLError as exc:
            last_error = exc
            if attempt + 1 < config.retry_attempts:
                _sleep_before_retry(config, attempt)
                continue
            raise

    if last_error is not None:
        raise last_error
    raise RuntimeError("request_json exhausted retries unexpectedly")


def _sleep_before_retry(config: ClientConfig, attempt: int) -> None:
    base_delay = config.retry_delay_seconds * (config.retry_backoff_factor ** attempt)
    jitter = random.uniform(0.0, max(0.0, config.retry_jitter_seconds))
    delay = min(config.max_retry_delay_seconds, base_delay + jitter)
    time.sleep(delay)
