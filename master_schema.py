from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


DEFAULT_SCHEMA_ENV = "DMANDER_MASTER_SCHEMA_PATH"
DEFAULT_PROJECT_SCHEMA = Path(__file__).with_name("dmander_master_schema_v01.json")


@dataclass(frozen=True)
class IntentSchema:
    intent_domain: str
    intent_type: str
    display_name: str
    required_fields: tuple[str, ...]
    optional_fields: tuple[str, ...]
    location_policy: dict[str, Any]
    budget_policy: str
    examples: tuple[str, ...]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "IntentSchema":
        return cls(
            intent_domain=data.get("intent_domain", ""),
            intent_type=data.get("intent_type", ""),
            display_name=data.get("display_name", data.get("intent_type", "")),
            required_fields=tuple(data.get("required_fields", [])),
            optional_fields=tuple(data.get("optional_fields", [])),
            location_policy=dict(data.get("location_policy", {})),
            budget_policy=data.get("budget_policy", "optional_range"),
            examples=tuple(data.get("examples", [])),
        )

    def to_prompt_line(self) -> str:
        allowed_locations = ", ".join(self.location_policy.get("allowed", [])) or "none"
        location_required = "sí" if self.location_policy.get("required") else "no"
        examples = " | ".join(self.examples[:2]) if self.examples else "-"
        return (
            f"- {self.intent_type} [{self.intent_domain}] "
            f"req={list(self.required_fields)} "
            f"opt={list(self.optional_fields)} "
            f"location_allowed=[{allowed_locations}] "
            f"location_required={location_required} "
            f"budget_policy={self.budget_policy} "
            f"examples={examples}"
        )

    def to_prompt_block(self) -> str:
        return (
            f"intent_domain: {self.intent_domain}\n"
            f"intent_type: {self.intent_type}\n"
            f"display_name: {self.display_name}\n"
            f"required_fields: {list(self.required_fields)}\n"
            f"optional_fields: {list(self.optional_fields)}\n"
            f"location_policy: {self.location_policy}\n"
            f"budget_policy: {self.budget_policy}\n"
            f"examples: {list(self.examples)}"
        )


class MasterSchemaRegistry:
    def __init__(self, source_path: Path, raw_schema: dict[str, Any]) -> None:
        self.source_path = source_path
        self.raw_schema = raw_schema
        self.version = raw_schema.get("version", "unknown")
        self.common_core = raw_schema.get("common_core", {})
        self.domains = {item["code"]: item.get("name", item["code"]) for item in raw_schema.get("domains", [])}
        self.fallback_policy = raw_schema.get("fallback_policy", {})
        self.intent_schemas = {
            item["intent_type"]: IntentSchema.from_dict(item)
            for item in raw_schema.get("intent_types", [])
            if item.get("intent_type")
        }
        fallback_intent = self.fallback_policy.get("default_intent_type", "unclassified_candidate")
        self.fallback_schema = self.intent_schemas.get(
            fallback_intent,
            IntentSchema(
                intent_domain=self.fallback_policy.get("default_domain", "special_opportunities"),
                intent_type=fallback_intent,
                display_name="Fallback",
                required_fields=("description",),
                optional_fields=tuple(self.fallback_policy.get("minimum_fields_to_extract", [])),
                location_policy={"allowed": ["unspecified"], "required": False},
                budget_policy="optional_range",
                examples=tuple(),
            ),
        )

    def resolve_intent_schema(self, intent_type: str | None) -> IntentSchema:
        if intent_type and intent_type in self.intent_schemas:
            return self.intent_schemas[intent_type]
        return self.fallback_schema

    def has_intent_type(self, intent_type: str | None) -> bool:
        return bool(intent_type and intent_type in self.intent_schemas)

    def has_domain(self, domain: str | None) -> bool:
        return bool(domain and domain in self.domains)

    def schema_prompt_catalog(self) -> str:
        return "\n".join(schema.to_prompt_line() for schema in self.intent_schemas.values())

    def active_schema_prompt(self, intent_type: str | None) -> str:
        return self.resolve_intent_schema(intent_type).to_prompt_block()

    def common_core_prompt(self) -> str:
        return json.dumps(self.common_core, ensure_ascii=False, indent=2)


def _candidate_schema_paths() -> list[Path]:
    paths: list[Path] = []
    env_path = os.getenv(DEFAULT_SCHEMA_ENV, "").strip()
    if env_path:
        paths.append(Path(env_path).expanduser())
    paths.append(DEFAULT_PROJECT_SCHEMA)
    return paths


@lru_cache(maxsize=1)
def get_master_schema_registry() -> MasterSchemaRegistry:
    for candidate in _candidate_schema_paths():
        if candidate.exists():
            raw_schema = json.loads(candidate.read_text(encoding="utf-8"))
            return MasterSchemaRegistry(candidate, raw_schema)
    searched = ", ".join(str(path) for path in _candidate_schema_paths())
    raise FileNotFoundError(
        "No se encontró el contrato maestro JSON de DMANDER. "
        f"Busca en: {searched}. "
        f"Puedes fijar la ruta con {DEFAULT_SCHEMA_ENV}."
    )
