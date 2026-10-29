"""Load and validate versioned JSON Schema contracts."""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from aus_hazard.config import CONTRACTS

QUAKE_SUBJECT = "quakes.raw-value"
HOTSPOT_SUBJECT = "hotspots.raw-value"


@lru_cache(maxsize=8)
def load_schema(name: str) -> dict[str, Any]:
    path = CONTRACTS / name
    schema = json.loads(path.read_text())
    Draft202012Validator.check_schema(schema)
    return schema


def validator_for(name: str) -> Draft202012Validator:
    return Draft202012Validator(load_schema(name), format_checker=Draft202012Validator.FORMAT_CHECKER)


def errors_for(payload: dict[str, Any], schema_name: str) -> list[str]:
    validator = validator_for(schema_name)
    out: list[str] = []
    for err in sorted(validator.iter_errors(payload), key=lambda item: list(item.path)):
        loc = ".".join(str(part) for part in err.path) or "$"
        out.append(f"{loc}: {err.message}")
    return out


def schema_for_quake(payload: dict[str, Any]) -> str:
    version = payload.get("schema_version")
    if version == 2:
        return "quake_event.v2.json"
    return "quake_event.v1.json"


def validate_quake(payload: dict[str, Any]) -> list[str]:
    return errors_for(payload, schema_for_quake(payload))


def validate_hotspot(payload: dict[str, Any]) -> list[str]:
    return errors_for(payload, "hotspot_event.v1.json")


def assert_valid(payload: dict[str, Any], kind: str) -> None:
    errors = validate_quake(payload) if kind == "quake" else validate_hotspot(payload)
    if errors:
        raise ValidationError("; ".join(errors))
