from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


DEFAULT_SCHEMA_ENV = "DMANDER_MASTER_SCHEMA_PATH"
DEFAULT_PROJECT_SCHEMA = Path(__file__).with_name("dmander_master_schema_v01.json")

ALLOWED_LOCATION_MODES = (
    "radius_from_point",
    "area",
    "city",
    "exact_address",
    "destination",
    "online",
    "hybrid",
    "multi_location",
    "unspecified",
)

FIELD_TYPE_LABELS = {
    "integer": "Numero Entero",
    "float": "Number Real",
    "date": "Fecha",
    "checkin_date": "CheckIn Date",
    "checkout_date": "CheckOut Date",
    "date_range": "Fecha",
    "time": "Hora-Minuto",
    "datetime": "Fecha-Hora",
    "text": "Texto",
    "money_eur": "Importe en Euros",
    "money_eur_range": "Importe en Euros",
    "enum": "Lista de Valores",
    "boolean": "Booleano",
    "location": "Ubicacion",
    "system_location": "System Location",
    "system_budget": "System Budget",
}

FIELD_TYPE_LABEL_TO_CODE: dict[str, str] = {}
for code, label in FIELD_TYPE_LABELS.items():
    FIELD_TYPE_LABEL_TO_CODE.setdefault(label.lower(), code)
FIELD_TYPE_LABEL_TO_CODE.setdefault("chackout date", "checkout_date")

BUDGET_UNITS = {"one-time", "per hour", "per day", "per night", "per season", "weekly", "monthly", "anual"}
REQUIRED_MODES = {"never", "always", "conditional"}
REQUIRED_OPERATORS = {"equals", "not_equals", "in"}


def _normalize_required_mode(value: Any) -> str:
    if isinstance(value, bool):
        return "always" if value else "never"
    raw = str(value or "").strip().lower()
    if raw in REQUIRED_MODES:
        return raw
    return "never"


def _normalize_condition_operator(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in REQUIRED_OPERATORS:
        return raw
    return "equals"


def _normalize_condition_values(value: Any) -> tuple[str, ...]:
    if isinstance(value, list):
        values = [str(item or "").strip() for item in value]
    else:
        values = [str(value or "").strip()]
    return tuple(item for item in values if item)


def _normalize_required_when(raw: Any) -> tuple[str, str, tuple[str, ...]]:
    if not isinstance(raw, dict):
        return "", "", tuple()
    field_name = str(raw.get("field") or "").strip()
    if not field_name:
        return "", "", tuple()
    operator = _normalize_condition_operator(raw.get("operator"))
    value = raw.get("value")
    if operator == "in":
        values = _normalize_condition_values(value if isinstance(value, list) else str(value or "").split(","))
    else:
        values = _normalize_condition_values(value)
    if not values:
        return "", "", tuple()
    return field_name, operator, values


def _value_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, dict):
        return any(_value_present(item) for item in value.values())
    if isinstance(value, list):
        return any(_value_present(item) for item in value)
    return True


def _normalize_condition_compare_values(value: Any) -> tuple[str, ...]:
    if isinstance(value, list):
        items = value
    elif isinstance(value, dict):
        return tuple()
    else:
        items = [value]
    output: list[str] = []
    for item in items:
        text = str(item or "").strip().lower()
        if text:
            output.append(text)
    return tuple(output)


def _required_status(
    required_mode: str,
    when_field: str,
    when_operator: str,
    when_values: tuple[str, ...],
    known_fields: dict[str, Any] | None,
) -> str:
    mode = _normalize_required_mode(required_mode)
    if mode == "always":
        return "active"
    if mode != "conditional":
        return "inactive"
    if not when_field:
        return "inactive"
    known = known_fields or {}
    current_value = known.get(when_field)
    if not _value_present(current_value):
        return "unresolved"
    actual_values = _normalize_condition_compare_values(current_value)
    if not actual_values:
        return "unresolved"
    expected_values = tuple(item.lower() for item in when_values if item)
    if not expected_values:
        return "inactive"
    matched = False
    if when_operator == "equals":
        matched = any(value == expected_values[0] for value in actual_values)
    elif when_operator == "not_equals":
        matched = all(value != expected_values[0] for value in actual_values)
    elif when_operator == "in":
        matched = any(value in expected_values for value in actual_values)
    return "active" if matched else "inactive"


@dataclass(frozen=True)
class BudgetPolicy:
    required_mode: str
    when_field: str
    when_operator: str
    when_values: tuple[str, ...]
    fix_or_range: str
    unit: str
    min_value: str
    max_value: str

    @classmethod
    def from_raw(cls, raw: Any) -> "BudgetPolicy":
        if not isinstance(raw, dict):
            return cls(required_mode="never", when_field="", when_operator="", when_values=tuple(), fix_or_range="", unit="", min_value="", max_value="")
        required_mode = _normalize_required_mode(raw.get("required"))
        when_field, when_operator, when_values = _normalize_required_when(raw.get("when"))
        fix_or_range = _normalize_fix_or_range(raw.get("fix_or_range"))
        unit = _normalize_budget_unit(raw.get("unit"))
        min_value = _normalize_budget_number(raw.get("min") if "min" in raw else raw.get("min_value"))
        max_value = _normalize_budget_number(raw.get("max") if "max" in raw else raw.get("max_value"))
        if required_mode == "never" and not fix_or_range:
            unit = ""
        elif fix_or_range and not unit:
            unit = "one-time"
        if required_mode in {"always", "conditional"} and not fix_or_range:
            fix_or_range = "fix"
        return cls(
            required_mode=required_mode,
            when_field=when_field,
            when_operator=when_operator,
            when_values=when_values,
            fix_or_range=fix_or_range,
            unit=unit,
            min_value=min_value,
            max_value=max_value,
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "required": self.required_mode,
            "fix_or_range": self.fix_or_range,
            "unit": self.unit,
            "validation": {
                "min": self.min_value,
                "max": self.max_value,
            },
        }
        if self.required_mode == "conditional" and self.when_field and self.when_values:
            payload["when"] = {
                "field": self.when_field,
                "operator": self.when_operator,
                "value": list(self.when_values) if self.when_operator == "in" else self.when_values[0],
            }
        return payload

    @property
    def required(self) -> bool:
        return self.required_mode == "always"

    def requirement_status(self, known_fields: dict[str, Any] | None = None) -> str:
        return _required_status(self.required_mode, self.when_field, self.when_operator, self.when_values, known_fields)

    def is_required_for(self, known_fields: dict[str, Any] | None = None) -> bool:
        return self.requirement_status(known_fields) == "active"

    @property
    def dependency_field(self) -> str:
        return self.when_field if self.required_mode == "conditional" else ""


@dataclass(frozen=True)
class RequirementPolicy:
    required_mode: str
    when_field: str
    when_operator: str
    when_values: tuple[str, ...]

    @classmethod
    def from_raw(cls, required: Any, when: Any = None) -> "RequirementPolicy":
        when_field, when_operator, when_values = _normalize_required_when(when)
        return cls(
            required_mode=_normalize_required_mode(required),
            when_field=when_field,
            when_operator=when_operator,
            when_values=when_values,
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"required": self.required_mode}
        if self.required_mode == "conditional" and self.when_field and self.when_values:
            payload["when"] = {
                "field": self.when_field,
                "operator": self.when_operator,
                "value": list(self.when_values) if self.when_operator == "in" else self.when_values[0],
            }
        return payload

    @property
    def required(self) -> bool:
        return self.required_mode == "always"

    def requirement_status(self, known_fields: dict[str, Any] | None = None) -> str:
        return _required_status(self.required_mode, self.when_field, self.when_operator, self.when_values, known_fields)

    def is_required_for(self, known_fields: dict[str, Any] | None = None) -> bool:
        return self.requirement_status(known_fields) == "active"


@dataclass(frozen=True)
class FieldSpec:
    name: str
    description: str
    value_type: str
    required: str
    when_field: str
    when_operator: str
    when_values: tuple[str, ...]
    choices: tuple[dict[str, str], ...]
    min_value: str
    max_value: str
    min_length: str
    max_length: str
    min_date: str
    max_date: str
    min_time: str
    max_time: str
    min_datetime: str
    max_datetime: str

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> "FieldSpec":
        name = str(raw.get("name") or "").strip()
        validation = raw.get("validation") or {}
        value_type = _normalize_field_value_type(raw.get("type"))
        choices = _normalize_choices(validation.get("options") or [])
        when_field, when_operator, when_values = _normalize_required_when(raw.get("when"))
        return cls(
            name=name,
            description=str(raw.get("description") or name).strip(),
            value_type=value_type,
            required=_normalize_required_mode(raw.get("required")),
            when_field=when_field,
            when_operator=when_operator,
            when_values=when_values,
            choices=tuple(choices),
            min_value=str(validation.get("min") or "").strip(),
            max_value=str(validation.get("max") or "").strip(),
            min_length=str(validation.get("min_length") or "").strip(),
            max_length=str(validation.get("max_length") or "").strip(),
            min_date=str(validation.get("min_date") or "").strip(),
            max_date=str(validation.get("max_date") or "").strip(),
            min_time=str(validation.get("min_time") or "").strip(),
            max_time=str(validation.get("max_time") or "").strip(),
            min_datetime=str(validation.get("min_datetime") or "").strip(),
            max_datetime=str(validation.get("max_datetime") or "").strip(),
        )

    def to_dict(self) -> dict[str, Any]:
        validation: dict[str, Any] = {}
        if self.choices:
            validation["options"] = [item["label"] for item in self.choices]
            validation["allow_custom"] = True
        if self.min_value:
            validation["min"] = self.min_value
        if self.max_value:
            validation["max"] = self.max_value
        if self.min_length:
            validation["min_length"] = self.min_length
        if self.max_length:
            validation["max_length"] = self.max_length
        if self.min_date:
            validation["min_date"] = self.min_date
        if self.max_date:
            validation["max_date"] = self.max_date
        if self.min_time:
            validation["min_time"] = self.min_time
        if self.max_time:
            validation["max_time"] = self.max_time
        if self.min_datetime:
            validation["min_datetime"] = self.min_datetime
        if self.max_datetime:
            validation["max_datetime"] = self.max_datetime
        payload = {
            "name": self.name,
            "type": FIELD_TYPE_LABELS.get(self.value_type, "Texto"),
            "description": self.description,
            "required": self.required,
            "validation": validation,
        }
        if self.required == "conditional" and self.when_field and self.when_values:
            payload["when"] = {
                "field": self.when_field,
                "operator": self.when_operator,
                "value": list(self.when_values) if self.when_operator == "in" else self.when_values[0],
            }
        return payload

    @property
    def label_es(self) -> str:
        return self.description

    @property
    def type_label(self) -> str:
        return FIELD_TYPE_LABELS.get(self.value_type, "Texto")

    @property
    def is_always_required(self) -> bool:
        return self.required == "always"

    @property
    def is_optional(self) -> bool:
        return self.required == "never"

    @property
    def is_conditional(self) -> bool:
        return self.required == "conditional"

    def requirement_status(self, known_fields: dict[str, Any] | None = None) -> str:
        return _required_status(self.required, self.when_field, self.when_operator, self.when_values, known_fields)

    def is_required_for(self, known_fields: dict[str, Any] | None = None) -> bool:
        return self.requirement_status(known_fields) == "active"


@dataclass(frozen=True)
class IntentSchema:
    intent_domain: str
    intent_type: str
    display_name: str
    fields: tuple[FieldSpec, ...]
    location_policy: RequirementPolicy
    budget_policy: BudgetPolicy
    has_location_field: bool
    has_budget_field: bool
    examples: tuple[str, ...]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "IntentSchema":
        raw_fields = [item for item in (data.get("fields") or []) if isinstance(item, dict) and str(item.get("name") or "").strip()]
        has_location_field = any(_is_location_system_field(item) for item in raw_fields)
        has_budget_field = any(_is_budget_system_field(item) for item in raw_fields)
        return cls(
            intent_domain=str(data.get("intent_domain") or "").strip(),
            intent_type=str(data.get("intent_type") or "").strip(),
            display_name=str(data.get("display_name") or data.get("intent_type") or "").strip(),
            fields=tuple(FieldSpec.from_raw(item) for item in raw_fields if not _is_system_field(item)),
            location_policy=_extract_location_policy(raw_fields, data),
            budget_policy=_extract_budget_policy(raw_fields, data),
            has_location_field=has_location_field,
            has_budget_field=has_budget_field,
            examples=tuple(data.get("examples") or []),
        )

    @property
    def required_field_specs(self) -> tuple[FieldSpec, ...]:
        return tuple(item for item in self.fields if item.is_always_required)

    @property
    def optional_field_specs(self) -> tuple[FieldSpec, ...]:
        return tuple(item for item in self.fields if item.is_optional)

    @property
    def required_fields(self) -> tuple[str, ...]:
        return tuple(item.name for item in self.required_field_specs)

    @property
    def optional_fields(self) -> tuple[str, ...]:
        return tuple(item.name for item in self.optional_field_specs)

    @property
    def location_required(self) -> bool:
        return self.location_policy.required

    @property
    def budget_required(self) -> bool:
        return self.budget_policy.required

    def active_required_field_specs(self, known_fields: dict[str, Any] | None = None) -> tuple[FieldSpec, ...]:
        return tuple(item for item in self.fields if item.is_required_for(known_fields))

    def active_required_fields(self, known_fields: dict[str, Any] | None = None) -> tuple[str, ...]:
        return tuple(item.name for item in self.active_required_field_specs(known_fields))

    def visible_optional_field_specs(self, known_fields: dict[str, Any] | None = None) -> tuple[FieldSpec, ...]:
        return tuple(
            item
            for item in self.fields
            if item.is_optional and item.requirement_status(known_fields) != "active"
        )

    def visible_optional_fields(self, known_fields: dict[str, Any] | None = None) -> tuple[str, ...]:
        return tuple(item.name for item in self.visible_optional_field_specs(known_fields))

    def conditional_dependency_fields(self, known_fields: dict[str, Any] | None = None) -> tuple[str, ...]:
        dependencies: list[str] = []
        for item in self.fields:
            if item.required != "conditional":
                continue
            if item.requirement_status(known_fields) != "unresolved":
                continue
            if item.when_field and item.when_field not in dependencies:
                dependencies.append(item.when_field)
        if self.location_requirement_status(known_fields) == "unresolved" and self.location_policy.when_field and self.location_policy.when_field not in dependencies:
            dependencies.append(self.location_policy.when_field)
        if self.budget_requirement_status(known_fields) == "unresolved" and self.budget_policy.when_field and self.budget_policy.when_field not in dependencies:
            dependencies.append(self.budget_policy.when_field)
        return tuple(dependencies)

    def location_required_for(self, known_fields: dict[str, Any] | None = None) -> bool:
        return self.location_policy.is_required_for(known_fields)

    def location_requirement_status(self, known_fields: dict[str, Any] | None = None) -> str:
        return self.location_policy.requirement_status(known_fields)

    def budget_required_for(self, known_fields: dict[str, Any] | None = None) -> bool:
        return self.budget_policy.is_required_for(known_fields)

    def budget_requirement_status(self, known_fields: dict[str, Any] | None = None) -> str:
        return self.budget_policy.requirement_status(known_fields)

    @property
    def budget_fix_or_range(self) -> str:
        return self.budget_policy.fix_or_range

    @property
    def budget_unit(self) -> str:
        return self.budget_policy.unit

    def field_spec(self, field_name: str) -> FieldSpec:
        for item in self.fields:
            if item.name == field_name:
                return item
        return FieldSpec(
            name=field_name,
            description=field_name,
            value_type="text",
            required="never",
            when_field="",
            when_operator="",
            when_values=tuple(),
            choices=tuple(),
            min_value="",
            max_value="",
            min_length="0",
            max_length="300",
            min_date="",
            max_date="",
            min_time="",
            max_time="",
            min_datetime="",
            max_datetime="",
        )

    def to_prompt_line(self) -> str:
        examples = " | ".join(self.examples[:2]) if self.examples else "-"
        return (
            f"- {self.intent_type} [{self.intent_domain}] "
            f"fields={len(self.fields)} "
            f"location_required={'sí' if self.location_required else 'no'} "
            f"budget_policy={self.budget_policy.to_dict()} "
            f"examples={examples}"
        )

    def to_prompt_block(self) -> str:
        return (
            f"intent_domain: {self.intent_domain}\n"
            f"intent_type: {self.intent_type}\n"
            f"display_name: {self.display_name}\n"
            f"fields: {[item.to_dict() for item in self.fields]}\n"
            f"location_policy: {self.location_policy.to_dict()}\n"
            f"budget_policy: {self.budget_policy.to_dict()}\n"
            f"examples: {list(self.examples)}"
        )


class MasterSchemaRegistry:
    def __init__(self, source_path: Path, raw_schema: dict[str, Any]) -> None:
        self.source_path = source_path
        self.raw_schema = raw_schema
        self.version = raw_schema.get("version", "unknown")
        self.domains = {item["code"]: item.get("name", item["code"]) for item in raw_schema.get("domains", [])}
        raw_intent_types = self._flatten_intent_types(raw_schema)
        self.intent_schemas = {
            item["intent_type"]: IntentSchema.from_dict(item)
            for item in raw_intent_types
            if item.get("intent_type")
        }
        self.common_core = self._derive_common_core()
        self.fallback_schema = IntentSchema(
            intent_domain="special_opportunities",
            intent_type="unclassified_candidate",
            display_name="Fallback",
            fields=tuple(),
            location_policy=RequirementPolicy.from_raw("never"),
            budget_policy=BudgetPolicy(required_mode="never", when_field="", when_operator="", when_values=tuple(), fix_or_range="", unit="", min_value="", max_value=""),
            has_location_field=False,
            has_budget_field=False,
            examples=tuple(),
        )

    def _derive_common_core(self) -> dict[str, Any]:
        fields = sorted({field.name for schema in self.intent_schemas.values() for field in schema.fields if field.name})
        field_types = sorted({field.value_type for schema in self.intent_schemas.values() for field in schema.fields if field.value_type})
        return {"fields": fields, "field_types": field_types}

    def _flatten_intent_types(self, raw_schema: dict[str, Any]) -> list[dict[str, Any]]:
        flattened: list[dict[str, Any]] = []
        for domain in raw_schema.get("domains", []) or []:
            domain_code = str(domain.get("code") or "").strip()
            for item in domain.get("intent_types", []) or []:
                if not isinstance(item, dict):
                    continue
                flattened.append(
                    {
                        **item,
                        "intent_domain": str(item.get("intent_domain") or domain_code).strip(),
                    }
                )
        return flattened

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


def _normalize_fix_or_range(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"fix", "fixed"}:
        return "fix"
    if raw in {"range", "rango"}:
        return "range"
    return ""


def _normalize_budget_unit(value: Any) -> str:
    raw = str(value or "").strip().lower()
    return raw if raw in BUDGET_UNITS else ""


def _normalize_budget_number(value: Any) -> str:
    raw = str(value or "").strip().replace(",", ".")
    if not raw:
        return ""
    try:
        number = float(raw)
    except ValueError:
        return ""
    return str(int(number)) if number.is_integer() else str(number)


def _normalize_field_value_type(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "text"
    return FIELD_TYPE_LABEL_TO_CODE.get(raw.lower(), raw)


def _normalize_choices(items: list[Any]) -> list[dict[str, str]]:
    choices: list[dict[str, str]] = []
    for item in items:
        value = str(item or "").strip()
        if value:
            choices.append({"value": value.lower(), "label": value})
    return choices


def _is_system_field(item: dict[str, Any]) -> bool:
    name = str(item.get("name") or "").strip()
    value_type = _normalize_field_value_type(item.get("type") or item.get("value_type") or "")
    return name in {"_location", "_budget"} or value_type in {"system_location", "system_budget"}


def _is_location_system_field(item: dict[str, Any]) -> bool:
    name = str(item.get("name") or "").strip()
    value_type = _normalize_field_value_type(item.get("type") or item.get("value_type") or "")
    return name == "_location" or value_type == "system_location"


def _is_budget_system_field(item: dict[str, Any]) -> bool:
    name = str(item.get("name") or "").strip()
    value_type = _normalize_field_value_type(item.get("type") or item.get("value_type") or "")
    return name == "_budget" or value_type == "system_budget"


def _extract_location_policy(raw_fields: list[dict[str, Any]], data: dict[str, Any]) -> RequirementPolicy:
    for item in raw_fields:
        name = str(item.get("name") or "").strip()
        value_type = _normalize_field_value_type(item.get("type") or item.get("value_type") or "")
        if name == "_location" or value_type == "system_location":
            return RequirementPolicy.from_raw(item.get("required"), item.get("when"))
    return RequirementPolicy.from_raw("never")


def _extract_budget_policy(raw_fields: list[dict[str, Any]], data: dict[str, Any]) -> BudgetPolicy:
    for item in raw_fields:
        name = str(item.get("name") or "").strip()
        value_type = _normalize_field_value_type(item.get("type") or item.get("value_type") or "")
        if name == "_budget" or value_type == "system_budget":
            validation = item.get("validation") if isinstance(item.get("validation"), dict) else {}
            return BudgetPolicy.from_raw(
                {
                    "required": item.get("required"),
                    "when": item.get("when"),
                    "fix_or_range": item.get("fix_or_range"),
                    "unit": item.get("unit"),
                    "min": validation.get("min"),
                    "max": validation.get("max"),
                }
            )
    return BudgetPolicy.from_raw(None)


def _candidate_schema_paths() -> list[Path]:
    paths: list[Path] = []
    env_path = os.getenv(DEFAULT_SCHEMA_ENV, "").strip()
    if env_path:
        paths.append(Path(env_path).expanduser())
    paths.append(DEFAULT_PROJECT_SCHEMA)
    return paths


@lru_cache(maxsize=4)
def _load_schema_registry_cached(path_str: str, mtime_ns: int) -> MasterSchemaRegistry:
    path = Path(path_str)
    raw_schema = json.loads(path.read_text(encoding="utf-8"))
    return MasterSchemaRegistry(path, raw_schema)


def get_master_schema_registry() -> MasterSchemaRegistry:
    for candidate in _candidate_schema_paths():
        if candidate.exists():
            stat = candidate.stat()
            return _load_schema_registry_cached(str(candidate.resolve()), stat.st_mtime_ns)
    searched = ", ".join(str(path) for path in _candidate_schema_paths())
    raise FileNotFoundError(
        "No se encontró el contrato maestro JSON de DMANDER. "
        f"Busca en: {searched}. "
        f"Puedes fijar la ruta con {DEFAULT_SCHEMA_ENV}."
    )


def _clear_master_schema_registry_cache() -> None:
    _load_schema_registry_cached.cache_clear()


get_master_schema_registry.cache_clear = _clear_master_schema_registry_cache  # type: ignore[attr-defined]
