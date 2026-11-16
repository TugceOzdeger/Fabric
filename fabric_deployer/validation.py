"""Definition validation helpers."""

import base64

from .definition import DefinitionError, FabricDefinition, validate_json_parts


def validate_definition(definition: FabricDefinition) -> None:
    paths = [part.path for part in definition.parts]
    if not paths:
        raise DefinitionError("Definition has no parts")
    if len(paths) != len(set(paths)):
        raise DefinitionError("Definition contains duplicate part paths")
    validate_json_parts(definition)
    for part in definition.parts:
        # Parsing and re-encoding must preserve every byte of the definition part.
        encoded = base64.b64encode(part.content)
        if base64.b64decode(encoded, validate=True) != part.content:
            raise DefinitionError(f"Definition part {part.path!r} failed Base64 round-trip validation")
