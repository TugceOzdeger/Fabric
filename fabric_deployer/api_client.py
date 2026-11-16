"""Minimal Microsoft Fabric REST API client."""

from __future__ import annotations

import logging
import time
from urllib.parse import urljoin, urlparse

import requests

from .auth import acquire_access_token
from .definition import FabricDefinition

LOG = logging.getLogger(__name__)
API_ROOT = "https://api.fabric.microsoft.com/v1"


class FabricAPIError(RuntimeError):
    def __init__(self, message: str, *, ambiguous: bool = False):
        super().__init__(message)
        self.ambiguous = ambiguous


class FabricAPIClient:
    def __init__(
        self,
        credential,
        *,
        session=None,
        base_url: str = API_ROOT,
        request_timeout: float = 30,
    ):
        self.credential = credential
        self.session = session or requests.Session()
        self.base_url = base_url.rstrip("/")
        self.request_timeout = request_timeout

    def get_definition(self, workspace_id: str, item_id: str) -> FabricDefinition:
        response = self._request(
            "POST",
            f"/workspaces/{workspace_id}/items/{item_id}/getDefinition",
            allow_accepted=True,
        )
        if response.status_code == 202:
            location = response.headers.get("Location")
            if not location:
                raise FabricAPIError(
                    "Fabric accepted the read request but supplied no operation location"
                )
            operation_url = self._validate_operation_url(location)
            response = self.poll_operation(
                operation_url,
                timeout=600,
                poll_interval=self._retry_after(response.headers.get("Retry-After"), 2),
                get_result=True,
            )
        try:
            body = response.json()
            return FabricDefinition.from_api(body.get("definition", body))
        except (ValueError, AttributeError) as exc:
            raise FabricAPIError("Fabric returned an invalid item definition response") from exc

    def submit_definition(
        self, workspace_id: str, item_id: str, definition: FabricDefinition
    ):
        try:
            response = self._request(
                "POST",
                f"/workspaces/{workspace_id}/items/{item_id}/updateDefinition",
                json={"definition": definition.to_api()},
                allow_accepted=True,
            )
        except FabricAPIError:
            raise
        except requests.RequestException as exc:
            raise FabricAPIError(
                "Update request outcome is unknown; inspect the live definition before retrying",
                ambiguous=True,
            ) from exc
        if response.status_code == 202:
            location = response.headers.get("Location") or response.headers.get("Operation-Location")
            if not location:
                raise FabricAPIError(
                    "Fabric accepted the update but supplied no operation location",
                    ambiguous=True,
                )
            return self._validate_operation_url(location)
        return None

    def poll_operation(
        self,
        operation_url: str,
        *,
        timeout: float,
        poll_interval: float,
        get_result: bool = False,
    ):
        deadline = time.monotonic() + timeout
        while True:
            response = self._request("GET", operation_url, absolute=True)
            try:
                body = response.json()
            except ValueError as exc:
                raise FabricAPIError("Fabric returned an invalid operation status") from exc
            status = str(body.get("status", "")).lower()
            if status in {"succeeded", "completed"}:
                if not get_result:
                    return response
                result_url = response.headers.get("Location")
                if not result_url:
                    result_url = f"{operation_url.rstrip('/')}/result"
                return self._request(
                    "GET", self._validate_operation_url(result_url), absolute=True
                )
            if status in {"failed", "cancelled"}:
                raise FabricAPIError(f"Fabric update operation {status}")
            if time.monotonic() >= deadline:
                raise FabricAPIError("Timed out waiting for the Fabric update operation")
            delay = self._retry_after(response.headers.get("Retry-After"), poll_interval)
            time.sleep(min(delay, max(0.0, deadline - time.monotonic())))

    def _validate_operation_url(self, location: str) -> str:
        operation_url = urljoin(f"{self.base_url}/", location)
        parsed_operation = urlparse(operation_url)
        if (
            parsed_operation.scheme != "https"
            or parsed_operation.netloc != urlparse(self.base_url).netloc
        ):
            raise FabricAPIError("Fabric returned an operation URL outside the API host")
        return operation_url

    @staticmethod
    def _retry_after(value: str | None, default: float) -> float:
        try:
            return max(0.0, float(value)) if value else default
        except ValueError:
            return default

    def _request(
        self, method: str, path: str, *, json=None, allow_accepted: bool = False, absolute=False
    ):
        token = acquire_access_token(self.credential)
        url = path if absolute else f"{self.base_url}{path}"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            response = self.session.request(
                method,
                url,
                headers=headers,
                json=json,
                timeout=self.request_timeout,
            )
        except requests.RequestException as exc:
            LOG.error("fabric_request_failed", extra={"method": method})
            raise
        accepted = {200, 201, 202, 204}
        if response.status_code not in accepted:
            ambiguous = method == "POST" and response.status_code >= 500
            LOG.error(
                "fabric_request_rejected",
                extra={"method": method, "status_code": response.status_code},
            )
            raise FabricAPIError(
                f"Fabric API request failed with HTTP {response.status_code}",
                ambiguous=ambiguous,
            )
        if response.status_code == 202 and not allow_accepted:
            return response
        return response
