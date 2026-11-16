"""Fabric item definition decoding, editing, and fingerprinting."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping


class DefinitionError(ValueError):
    """An item definition is malformed or cannot be safely edited."""


@dataclass(frozen=True)
class DefinitionPart:
    path: str
    content: bytes
    payload_type: str = "InlineBase64"


@dataclass(frozen=True)
class FabricDefinition:
    parts: tuple[DefinitionPart, ...]
    format: str | None = None

    @classmethod
    def from_api(cls, value: Mapping[str, Any]) -> "FabricDefinition":
        if not isinstance(value, Mapping):
            raise DefinitionError("Definition response must be an object")
        raw_parts = value.get("parts")
        if not isinstance(raw_parts, list):
            raise DefinitionError("Definition response does not contain a parts list")

        parts = []
        seen_paths = set()
        for raw_part in raw_parts:
            if not isinstance(raw_part, Mapping):
                raise DefinitionError("Definition contains a malformed part")
            path = raw_part.get("path")
            payload = raw_part.get("payload")
            payload_type = raw_part.get("payloadType")
            if not isinstance(path, str) or not path or path in seen_paths:
                raise DefinitionError("Definition contains an invalid or duplicate part path")
            if payload_type != "InlineBase64" or not isinstance(payload, str):
                raise DefinitionError(f"Definition part {path!r} is not InlineBase64")
            try:
                content = base64.b64decode(payload, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise DefinitionError(f"Definition part {path!r} has invalid Base64") from exc
            if base64.b64encode(content).decode("ascii") != payload:
                raise DefinitionError(f"Definition part {path!r} failed Base64 round-trip validation")
            seen_paths.add(path)
            parts.append(DefinitionPart(path, content, payload_type))

        format_value = value.get("format")
        if format_value is not None and not isinstance(format_value, str):
            raise DefinitionError("Definition format must be a string")
        definition = cls(tuple(parts), format_value)
        validate_json_parts(definition)
        return definition

    def to_api(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "parts": [
                {
                    "path": part.path,
                    "payload": base64.b64encode(part.content).decode("ascii"),
                    "payloadType": part.payload_type,
                }
                for part in self.parts
            ]
        }
        if self.format is not None:
            result["format"] = self.format
        return result

    def fingerprint(self) -> str:
        digest = hashlib.sha256()
        digest.update(json.dumps(self.format, ensure_ascii=False).encode("utf-8"))
        for part in sorted(self.parts, key=lambda item: item.path):
            path = part.path.encode("utf-8")
            digest.update(len(path).to_bytes(8, "big"))
            digest.update(path)
            digest.update(len(part.content).to_bytes(8, "big"))
            digest.update(part.content)
        return digest.hexdigest()


def validate_json_parts(definition: FabricDefinition) -> None:
    for part in definition.parts:
        if part.path.lower().endswith(".json"):
            try:
                json.loads(part.content.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise DefinitionError(f"Definition part {part.path!r} is not valid UTF-8 JSON") from exc


def apply_json_edits(
    definition: FabricDefinition,
    edits: Mapping[str, list[Mapping[str, Any]]],
) -> FabricDefinition:
    """Apply explicit JSON Pointer set operations while retaining other definition parts."""
    edits_by_path = {path: operations for path, operations in edits.items()}
    unknown_paths = set(edits_by_path) - {part.path for part in definition.parts}
    if unknown_paths:
        raise DefinitionError(f"JSON edits refer to missing part {sorted(unknown_paths)[0]!r}")

    result = []
    for part in definition.parts:
        operations = edits_by_path.get(part.path)
        if operations is None:
            result.append(part)
            continue
        try:
            document = json.loads(part.content.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DefinitionError(f"Cannot edit non-JSON definition part {part.path!r}") from exc
        if not isinstance(operations, list):
            raise DefinitionError(f"Edits for {part.path!r} must be a list")
        for operation in operations:
            if not isinstance(operation, Mapping) or operation.get("op") != "set":
                raise DefinitionError("Only explicit JSON Pointer 'set' edits are supported")
            pointer = operation.get("pointer")
            if not isinstance(pointer, str) or not pointer.startswith("/"):
                raise DefinitionError("JSON Pointer edits must start with '/'")
            tokens = [
                token.replace("~1", "/").replace("~0", "~")
                for token in pointer[1:].split("/")
            ]
            parent = document
            for token in tokens[:-1]:
                if isinstance(parent, dict) and token in parent:
                    parent = parent[token]
                elif isinstance(parent, list) and token.isdecimal() and int(token) < len(parent):
                    parent = parent[int(token)]
                else:
                    raise DefinitionError(f"JSON Pointer {pointer!r} does not exist")
            leaf = tokens[-1]
            if isinstance(parent, dict) and leaf in parent:
                parent[leaf] = operation.get("value")
            elif isinstance(parent, list) and leaf.isdecimal() and int(leaf) < len(parent):
                parent[int(leaf)] = operation.get("value")
            else:
                raise DefinitionError(f"JSON Pointer {pointer!r} does not exist")

        content = json.dumps(document, indent=2, ensure_ascii=False).encode("utf-8")
        result.append(DefinitionPart(part.path, content, part.payload_type))

    edited = FabricDefinition(tuple(result), definition.format)
    validate_json_parts(edited)
    return edited
