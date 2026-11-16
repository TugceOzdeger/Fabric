"""Dry-run and guarded deployment orchestration."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

import requests

from .api_client import FabricAPIError
from .auth import AuthenticationError
from .definition import DefinitionError, FabricDefinition, apply_json_edits
from .validation import validate_definition

LOG = logging.getLogger(__name__)


class ConcurrentModificationError(RuntimeError):
    """The live item changed after the deployment baseline was read."""


class AmbiguousUpdateError(RuntimeError):
    """An update may have reached Fabric, but its final state is not known."""


@dataclass(frozen=True)
class DeploymentResult:
    mode: str
    baseline_fingerprint: str
    desired_fingerprint: str
    changed: bool
    verified: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "baseline_fingerprint": self.baseline_fingerprint,
            "desired_fingerprint": self.desired_fingerprint,
            "changed": self.changed,
            "verified": self.verified,
        }


def deploy_definition(
    client,
    workspace_id: str,
    item_id: str,
    edits: Mapping[str, list[Mapping[str, Any]]],
    *,
    apply: bool = False,
    expected_fingerprint: str | None = None,
    operation_timeout: float = 600,
    poll_interval: float = 2,
) -> DeploymentResult:
    baseline = client.get_definition(workspace_id, item_id)
    validate_definition(baseline)
    baseline_fingerprint = baseline.fingerprint()
    if expected_fingerprint and expected_fingerprint != baseline_fingerprint:
        raise ConcurrentModificationError(
            "Live definition does not match --expected-fingerprint; no update was submitted"
        )

    desired = apply_json_edits(baseline, edits)
    validate_definition(desired)
    desired_fingerprint = desired.fingerprint()
    changed = desired_fingerprint != baseline_fingerprint
    if not apply:
        LOG.info("dry_run_complete", extra={"changed": changed})
        return DeploymentResult(
            "dry-run", baseline_fingerprint, desired_fingerprint, changed, False
        )
    if not changed:
        return DeploymentResult(
            "apply", baseline_fingerprint, desired_fingerprint, False, True
        )

    latest = client.get_definition(workspace_id, item_id)
    validate_definition(latest)
    if latest.fingerprint() != baseline_fingerprint:
        raise ConcurrentModificationError(
            "Live definition changed during preparation; no update was submitted"
        )

    try:
        operation_url = client.submit_definition(workspace_id, item_id, desired)
        if operation_url:
            client.poll_operation(
                operation_url, timeout=operation_timeout, poll_interval=poll_interval
            )
    except FabricAPIError as exc:
        if not exc.ambiguous:
            raise
        try:
            observed = client.get_definition(workspace_id, item_id)
        except (
            AuthenticationError,
            DefinitionError,
            FabricAPIError,
            requests.RequestException,
        ) as read_error:
            raise AmbiguousUpdateError(
                "Update outcome is unknown and the live definition could not be checked; "
                "do not retry until its state is verified"
            ) from read_error
        if observed.fingerprint() != desired_fingerprint:
            raise AmbiguousUpdateError(
                "Update outcome is unknown and the live definition does not match the "
                "requested definition; no automatic retry was made"
            ) from exc

    final_definition = client.get_definition(workspace_id, item_id)
    if final_definition.fingerprint() != desired_fingerprint:
        raise FabricAPIError("Final live definition does not match the requested definition")
    LOG.info("deployment_verified", extra={"item_id": item_id})
    return DeploymentResult(
        "apply", baseline_fingerprint, desired_fingerprint, True, True
    )
