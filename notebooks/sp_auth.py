"""Cached, auto-refreshing OAuth M2M auth for calling Databricks (e.g. an App) as a service principal.

M2M access tokens last one hour, and the client-credentials grant issues no refresh token;
"refreshing" means running the grant again. The Databricks SDK already does that well, so this
module only keeps one SDK ``Config`` per SP and exposes it for plain HTTP callers:

* the token is reused across requests (thread-safe),
* it is refreshed in the background during the last ``min(TTL/2, 20 min)`` of its life,
* a request only blocks on a refresh if the token is within 40 s of expiring,
* ``SPAuth`` retries once with a fresh token if a request comes back 401.

Usage::

    auth = SPAuth(host, client_id, client_secret)
    requests.post(f"{app_url}/responses", auth=auth, json=payload)

    # Or wherever you need raw headers (e.g. per-call ``extra_headers`` on an OpenAI client):
    headers = sp_auth_headers(host, client_id, client_secret)
"""

from __future__ import annotations

import threading

import requests
from databricks.sdk.config import Config

_configs: dict[tuple[str, str, str], Config] = {}
_lock = threading.Lock()


def sp_config(host: str, client_id: str, client_secret: str) -> Config:
    """The one SDK Config (and so the one cached token) for this SP."""
    key = (host.rstrip("/"), client_id, client_secret)
    with _lock:
        if key not in _configs:
            _configs[key] = Config(
                host=key[0], client_id=client_id, client_secret=client_secret, auth_type="oauth-m2m"
            )
        return _configs[key]


def sp_auth_headers(host: str, client_id: str, client_secret: str) -> dict[str, str]:
    """``{"Authorization": "Bearer <token>"}``: cached, refreshed before expiry."""
    return sp_config(host, client_id, client_secret).authenticate()


def invalidate(host: str, client_id: str) -> None:
    """Drop the cached token(s) for an SP; the next call fetches a new one."""
    with _lock:
        for key in [k for k in _configs if k[:2] == (host.rstrip("/"), client_id)]:
            del _configs[key]


class SPAuth(requests.auth.AuthBase):
    """``requests`` auth that signs each request as the SP and retries once on 401."""

    def __init__(self, host: str, client_id: str, client_secret: str) -> None:
        self.host, self.client_id, self.client_secret = host, client_id, client_secret

    def __call__(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
        request.headers.update(sp_auth_headers(self.host, self.client_id, self.client_secret))
        request.register_hook("response", self._retry_on_401)
        return request

    def _retry_on_401(self, response: requests.Response, **kwargs) -> requests.Response:
        if response.status_code != 401 or response.request.headers.get("X-SP-Auth-Retry"):
            return response
        # The token was rejected (e.g. revoked early): fetch a new one and retry once.
        invalidate(self.host, self.client_id)
        response.content  # drain so the connection can be reused
        response.close()
        retry = response.request.copy()
        retry.headers.update(sp_auth_headers(self.host, self.client_id, self.client_secret))
        retry.headers["X-SP-Auth-Retry"] = "1"
        new_response = response.connection.send(retry, **kwargs)
        new_response.history.append(response)
        return new_response
