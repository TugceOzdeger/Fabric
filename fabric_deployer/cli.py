"""Command-line entry point for fabric-deployer."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from .api_client import FabricAPIClient
from .auth import create_credential
from .deployment import deploy_definition


class JSONFormatter(logging.Formatter):
    def format(self, record):
        entry = {"level": record.levelname.lower(), "message": record.getMessage()}
        for field in ("item_id", "method", "status_code", "changed"):
            if hasattr(record, field):
                entry[field] = getattr(record, field)
        return json.dumps(entry, separators=(",", ":"))


def configure_logging(verbose: bool = False) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    logging.getLogger("azure").setLevel(logging.ERROR)


def _read_config(path: str) -> dict:
    try:
        config = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Unable to read valid JSON configuration from {path!r}") from exc
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a JSON object")
    required = ("workspace_id", "item_id")
    if any(not isinstance(config.get(key), str) or not config[key] for key in required):
        raise ValueError("Configuration requires non-empty workspace_id and item_id")
    edits = config.get("json_edits", {})
    if not isinstance(edits, dict):
        raise ValueError("json_edits must be an object keyed by definition part path")
    return config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fabric-deployer",
        description="Safely inspect and deploy Microsoft Fabric item definitions.",
    )
    parser.add_argument("--config", required=True, help="Path to a JSON deployment configuration")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Submit the definition update (default: dry-run only)",
    )
    parser.add_argument(
        "--expected-fingerprint",
        help="Abort unless the retrieved live definition has this SHA-256 fingerprint",
    )
    parser.add_argument("--timeout", type=float, default=600, help="Async operation timeout in seconds")
    parser.add_argument("--poll-interval", type=float, default=2, help="Async poll interval in seconds")
    parser.add_argument("--verbose", action="store_true", help="Enable structured debug logging")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.verbose)
    try:
        config = _read_config(args.config)
        client = FabricAPIClient(create_credential())
        result = deploy_definition(
            client,
            config["workspace_id"],
            config["item_id"],
            config.get("json_edits", {}),
            apply=args.apply,
            expected_fingerprint=args.expected_fingerprint,
            operation_timeout=args.timeout,
            poll_interval=args.poll_interval,
        )
        print(json.dumps(result.to_dict(), sort_keys=True))
        return 0
    except Exception as exc:
        print(f"fabric-deployer: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
