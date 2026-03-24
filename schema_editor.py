from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Optional

from field_specs import is_budget_field, is_date_field, is_integer_field, is_location_field, is_number_field
from master_schema import DEFAULT_PROJECT_SCHEMA, get_master_schema_registry
from normalization_rules import FIELD_LABELS


FIELD_VALUE_TYPES = [
    "text",
    "integer",
    "float",
    "date",
    "checkin_date",
    "checkout_date",
    "date_range",
    "time",
    "datetime",
    "money_eur",
    "money_eur_range",
    "enum",
    "boolean",
    "location",
    "system_location",
    "system_budget",
]

TYPE_LABELS = {
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

TYPE_LABEL_TO_CODE: dict[str, str] = {}
for code, label in TYPE_LABELS.items():
    TYPE_LABEL_TO_CODE.setdefault(label.lower(), code)
TYPE_LABEL_TO_CODE.setdefault("chackout date", "checkout_date")

BUDGET_UNITS = [
    "one-time",
    "per hour",
    "per day",
    "per night",
    "per season",
    "weekly",
    "monthly",
    "anual",
]
REQUIRED_MODES = ["never", "always", "conditional"]
REQUIRED_OPERATORS = ["equals", "not_equals", "in"]

PREDEFINED_TYPES = [
    {
        "value": "integer",
        "label": "Numero Entero",
        "description": "Entero con validación opcional entre dos valores.",
        "validation_fields": ["min", "max"],
    },
    {
        "value": "float",
        "label": "Number Real",
        "description": "Número real con validación opcional entre dos valores float.",
        "validation_fields": ["min", "max"],
    },
    {
        "value": "date",
        "label": "Fecha",
        "description": "Fecha con validación opcional entre dos fechas.",
        "validation_fields": ["min_date", "max_date"],
    },
    {
        "value": "checkin_date",
        "label": "CheckIn Date",
        "description": "Fecha de entrada. Debe ser hoy o posterior.",
        "validation_fields": ["min_date"],
    },
    {
        "value": "checkout_date",
        "label": "CheckOut Date",
        "description": "Fecha de salida. Debe ser posterior al CheckIn Date.",
        "validation_fields": ["min_date"],
    },
    {
        "value": "time",
        "label": "Hora-Minuto",
        "description": "Hora-Minuto con validación opcional entre dos horas.",
        "validation_fields": ["min_time", "max_time"],
    },
    {
        "value": "datetime",
        "label": "Fecha-Hora",
        "description": "Fecha-Hora con validación opcional entre dos fechas-hora.",
        "validation_fields": ["min_datetime", "max_datetime"],
    },
    {
        "value": "text",
        "label": "Texto",
        "description": "Texto con validación opcional por número de caracteres.",
        "validation_fields": ["min_length", "max_length"],
    },
    {
        "value": "money_eur",
        "label": "Importe en Euros",
        "description": "Importe en euros con validación opcional entre dos importes.",
        "validation_fields": ["min", "max"],
    },
    {
        "value": "enum",
        "label": "Lista de Valores",
        "description": "Una opción entre una lista predeterminada.",
        "validation_fields": ["options"],
    },
    {
        "value": "boolean",
        "label": "Booleano",
        "description": "Verdadero o falso.",
        "validation_fields": [],
    },
    {
        "value": "location",
        "label": "Ubicacion",
        "description": "Coordenadas geográficas y zona seleccionada en mapa.",
        "validation_fields": [],
    },
]

SELECT_FIELD_DEFAULTS: dict[str, list[dict[str, str]]] = {
    "modality": [
        {"value": "presencial", "label": "Presencial"},
        {"value": "online", "label": "Online"},
        {"value": "hibrida", "label": "Híbrida"},
        {"value": "me da igual", "label": "Me da igual"},
    ],
    "service_mode": [
        {"value": "presencial", "label": "Presencial"},
        {"value": "online", "label": "Online"},
        {"value": "a domicilio", "label": "A domicilio"},
        {"value": "me da igual", "label": "Me da igual"},
    ],
    "urgency": [
        {"value": "lo necesito ya", "label": "Lo necesito ya"},
        {"value": "esta semana", "label": "Esta semana"},
        {"value": "en los proximos dias", "label": "En los próximos días"},
        {"value": "sin prisa", "label": "Sin prisa"},
    ],
}


class SchemaEditorError(Exception):
    pass


def load_editable_schema(path: Optional[Path] = None) -> dict[str, Any]:
    target = path or DEFAULT_PROJECT_SCHEMA
    raw = json.loads(target.read_text(encoding="utf-8"))
    return normalize_editable_schema(raw)


def save_editable_schema(raw_schema: dict[str, Any], path: Optional[Path] = None) -> None:
    target = path or DEFAULT_PROJECT_SCHEMA
    normalized = normalize_editable_schema(raw_schema)
    target.write_text(json.dumps(normalized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    get_master_schema_registry.cache_clear()


def normalize_editable_schema(raw_schema: dict[str, Any]) -> dict[str, Any]:
    raw = deepcopy(raw_schema)
    raw.setdefault("version", "2.0")
    raw.setdefault("description", "Master schema simplificado de DMANDER")
    raw.setdefault("domains", [])

    domains: list[dict[str, str]] = []
    seen_domains: set[str] = set()
    for item in raw.get("domains", []):
        code = str(item.get("code") or "").strip()
        name = str(item.get("name") or "").strip()
        if not code or not name:
            continue
        if code in seen_domains:
            raise SchemaEditorError(f"El dominio '{code}' está duplicado.")
        seen_domains.add(code)
        domain_payload = {"code": code, "name": name, "intent_types": []}
        domains.append(domain_payload)

    domain_lookup = {item["code"]: item for item in domains}
    seen_types: set[str] = set()
    for domain_item in raw.get("domains", []):
        domain_code = str(domain_item.get("code") or "").strip()
        target_domain = domain_lookup.get(domain_code)
        if not target_domain:
            continue
        normalized_intents: list[dict[str, Any]] = []
        for item in domain_item.get("intent_types", []) or []:
            normalized = _normalize_intent_item(item, seen_domains, domain_code)
            intent_type = normalized["intent_type"]
            if intent_type in seen_types:
                raise SchemaEditorError(f"El intent_type '{intent_type}' está duplicado.")
            seen_types.add(intent_type)
            normalized_intents.append(normalized)
        target_domain["intent_types"] = normalized_intents

    raw["domains"] = domains
    raw.pop("intent_types", None)
    raw.pop("common_core", None)
    raw.pop("fallback_policy", None)
    raw.pop("field_definitions", None)
    return raw


def schema_editor_context() -> dict[str, Any]:
    normalized = load_editable_schema()
    domains = normalized.get("domains", [])
    grouped_domains = []
    for domain in domains:
        intent_types = []
        for item in domain.get("intent_types", []) or []:
            system = _extract_system_policies(item)
            intent_types.append(
                {
                    **item,
                    "domain_name": domain["name"],
                    "location_policy": system["location_required"],
                    "budget_policy": system["budget_policy"],
                    "budget_policy_display": _budget_policy_display(system["budget_policy"]),
                    "field_specs": _field_rows_from_entries(item.get("fields") or []),
                }
            )
        grouped_domains.append({**domain, "intent_types": intent_types})
    return {
        "schema_path": str(DEFAULT_PROJECT_SCHEMA),
        "domains": domains,
        "domain_groups": grouped_domains,
        "field_type_catalog": field_type_catalog(),
        "budget_unit_catalog": budget_unit_catalog(),
        "budget_range_catalog": budget_range_catalog(),
        "required_mode_catalog": required_mode_catalog(),
        "condition_operator_catalog": condition_operator_catalog(),
        "predefined_types": predefined_type_catalog(),
        "default_field_specs": default_system_field_rows(),
    }


def create_domain(code: str, name: str) -> None:
    raw = load_editable_schema()
    raw.setdefault("domains", []).append({"code": code, "name": name, "intent_types": []})
    save_editable_schema(raw)


def update_domain(original_code: str, code: str, name: str) -> None:
    raw = load_editable_schema()
    found = False
    for item in raw.setdefault("domains", []):
        if str(item.get("code")) == str(original_code):
            item["code"] = code
            item["name"] = name
            for intent in item.get("intent_types", []) or []:
                intent["intent_domain"] = code
            found = True
    if not found:
        raise SchemaEditorError("No he encontrado ese dominio.")
    save_editable_schema(raw)


def delete_domain(code: str) -> None:
    raw = load_editable_schema()
    if any(item.get("intent_types") for item in raw.get("domains", []) if (item.get("code") or "") == code):
        raise SchemaEditorError("No puedes borrar un dominio que todavía tiene intent_type asociados.")
    raw["domains"] = [item for item in raw.get("domains", []) if (item.get("code") or "") != code]
    save_editable_schema(raw)


def reorder_domains(domain_codes: list[str]) -> None:
    raw = load_editable_schema()
    domains = list(raw.get("domains", []) or [])
    if not domains:
        return
    by_code = {str(item.get("code") or "").strip(): item for item in domains}
    requested = [code for code in domain_codes if code in by_code]
    if len(requested) != len(domains):
        missing = [code for code in by_code if code not in requested]
        requested.extend(missing)
    raw["domains"] = [by_code[code] for code in requested]
    save_editable_schema(raw)


def reorder_intent_types(domain_code: str, intent_types: list[str]) -> None:
    raw = load_editable_schema()
    for domain in raw.get("domains", []):
        if str(domain.get("code") or "").strip() != str(domain_code or "").strip():
            continue
        current = list(domain.get("intent_types", []) or [])
        if not current:
            return
        by_code = {str(item.get("intent_type") or "").strip(): item for item in current}
        requested = [code for code in intent_types if code in by_code]
        if len(requested) != len(current):
            missing = [code for code in by_code if code not in requested]
            requested.extend(missing)
        domain["intent_types"] = [by_code[code] for code in requested]
        save_editable_schema(raw)
        return
    raise SchemaEditorError("No he encontrado el dominio para reordenar sus intent_type.")


def create_intent_type(payload: dict[str, Any]) -> None:
    raw = load_editable_schema()
    domain_code = str(payload.get("intent_domain") or "").strip()
    for domain in raw.get("domains", []):
        if str(domain.get("code") or "") == domain_code:
            domain.setdefault("intent_types", []).append(payload)
            save_editable_schema(raw)
            return
    raise SchemaEditorError("No he encontrado el dominio para crear ese intent_type.")


def update_intent_type(original_intent_type: str, payload: dict[str, Any]) -> None:
    raw = load_editable_schema()
    found = False
    target_domain = str(payload.get("intent_domain") or "").strip()
    for domain in raw.get("domains", []):
        intents = domain.setdefault("intent_types", [])
        for index, item in enumerate(intents):
            if str(item.get("intent_type")) == str(original_intent_type):
                del intents[index]
                found = True
                break
        if found:
            break
    if not found:
        raise SchemaEditorError("No he encontrado ese intent_type.")
    for domain in raw.get("domains", []):
        if str(domain.get("code") or "") == target_domain:
            domain.setdefault("intent_types", []).append(payload)
            save_editable_schema(raw)
            return
    raise SchemaEditorError("No he encontrado el dominio destino para ese intent_type.")


def delete_intent_type(intent_type: str) -> None:
    raw = load_editable_schema()
    found = False
    for domain in raw.get("domains", []):
        intents = domain.get("intent_types", []) or []
        filtered = [item for item in intents if (item.get("intent_type") or "") != intent_type]
        if len(filtered) != len(intents):
            domain["intent_types"] = filtered
            found = True
            break
    if not found:
        raise SchemaEditorError("No he encontrado ese intent_type.")
    save_editable_schema(raw)


def field_type_catalog() -> list[dict[str, str]]:
    catalog = [{"value": item["label"], "label": item["label"]} for item in PREDEFINED_TYPES]
    catalog.extend(
        [
            {"value": "System Location", "label": "System Location"},
            {"value": "System Budget", "label": "System Budget"},
        ]
    )
    return catalog


def predefined_type_catalog() -> list[dict[str, Any]]:
    return deepcopy(PREDEFINED_TYPES)


def budget_unit_catalog() -> list[dict[str, str]]:
    labels = {
        "one-time": "One-time",
        "per hour": "Por hora",
        "per day": "Por día",
        "per night": "Por noche",
        "per season": "Por sesión",
        "weekly": "Semanal",
        "monthly": "Mensual",
        "anual": "Anual",
    }
    return [{"value": item, "label": labels[item]} for item in BUDGET_UNITS]


def budget_range_catalog() -> list[dict[str, str]]:
    return [
        {"value": "fix", "label": "Precio máximo"},
        {"value": "range", "label": "Rango de precios"},
    ]


def required_mode_catalog() -> list[dict[str, str]]:
    return [
        {"value": "never", "label": "No obligatorio"},
        {"value": "always", "label": "Siempre obligatorio"},
        {"value": "conditional", "label": "Condicional"},
    ]


def condition_operator_catalog() -> list[dict[str, str]]:
    return [
        {"value": "equals", "label": "Es igual a"},
        {"value": "not_equals", "label": "No es igual a"},
        {"value": "in", "label": "Está en la lista"},
    ]


def default_system_field_rows() -> list[dict[str, Any]]:
    return _field_rows_from_entries(
        [
            {
                "name": "_location",
                "type": "System Location",
                "description": "Ubicación de la demanda",
                "required": "never",
                "validation": {},
            },
            {
                "name": "_budget",
                "type": "System Budget",
                "description": "Presupuesto de la demanda",
                "required": "never",
                "fix_or_range": "fix",
                "unit": "",
                "validation": {"min": "", "max": ""},
            },
        ]
    )


def get_field_definition(field_name: str, intent_type: str = "") -> dict[str, Any]:
    if intent_type:
        schema = get_master_schema_registry().resolve_intent_schema(intent_type)
        spec = schema.field_spec(field_name)
        if spec.name == field_name:
            return _normalize_field_definition(field_name, _field_spec_to_definition_dict(spec))
    return _default_field_definition(field_name)


def _normalize_intent_item(item: dict[str, Any], known_domains: set[str], default_domain: str = "") -> dict[str, Any]:
    intent_domain = str(item.get("intent_domain") or default_domain).strip()
    if not intent_domain or intent_domain not in known_domains:
        raise SchemaEditorError(f"El dominio '{intent_domain or '-'}' no existe.")
    intent_type = str(item.get("intent_type") or "").strip()
    display_name = str(item.get("display_name") or "").strip()
    if not intent_type or not display_name:
        raise SchemaEditorError("Cada intent_type necesita código y nombre visible.")
    fields = _normalize_fields_for_intent(item)
    return {
        "intent_domain": intent_domain,
        "intent_type": intent_type,
        "display_name": display_name,
        "fields": fields,
        "examples": _normalize_string_list(item.get("examples") or []),
    }


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


def _normalize_condition_value_text(value: Any, operator: str) -> str:
    if isinstance(value, list):
        items = [str(item or "").strip() for item in value]
    else:
        items = [str(value or "").strip()]
    cleaned = [item for item in items if item]
    if operator == "in":
        return ", ".join(cleaned)
    return cleaned[0] if cleaned else ""


def _normalize_required_when(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {"field": "", "operator": "equals", "value": ""}
    field_name = str(value.get("field") or "").strip()
    operator = _normalize_condition_operator(value.get("operator"))
    value_text = _normalize_condition_value_text(value.get("value"), operator)
    if not field_name or not value_text:
        return {"field": "", "operator": "equals", "value": ""}
    return {"field": field_name, "operator": operator, "value": value_text}


def _normalize_location_policy(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {
            "required": _normalize_required_mode(value.get("required")),
            "when": _normalize_required_when(value.get("when")),
        }
    return {
        "required": _normalize_required_mode(value),
        "when": {"field": "", "operator": "equals", "value": ""},
    }


def _normalize_budget_policy(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        required = _normalize_required_mode(value.get("required"))
        when = _normalize_required_when(value.get("when"))
        fix_or_range = _normalize_fix_or_range(value.get("fix_or_range"))
        unit = _normalize_budget_unit(value.get("unit") or value.get("units"))
        min_value = _normalize_number_string(value.get("min") or value.get("min_value") or "")
        max_value = _normalize_number_string(value.get("max") or value.get("max_value") or "")
    else:
        required, when, fix_or_range, unit, min_value, max_value = "never", {"field": "", "operator": "equals", "value": ""}, "", "", "", ""
    if required == "never":
        if not fix_or_range:
            unit = ""
        elif not unit:
            unit = "one-time"
    else:
        fix_or_range = fix_or_range or "fix"
        unit = unit or "one-time"
    return {
        "required": required,
        "when": when,
        "fix_or_range": fix_or_range,
        "unit": unit,
        "min": min_value,
        "max": max_value,
    }


def _normalize_fields_for_intent(item: dict[str, Any]) -> list[dict[str, Any]]:
    raw_fields = item.get("fields")
    if not isinstance(raw_fields, list):
        raise SchemaEditorError("Cada intent_type debe definir sus campos dentro de 'fields'.")
    normalized_fields: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_field in raw_fields:
        normalized = _normalize_field_entry(raw_field)
        if normalized["name"] in seen:
            continue
        seen.add(normalized["name"])
        normalized_fields.append(normalized)
    system_entries = _system_field_entries_from_item(item, normalized_fields)
    regular_fields = [field for field in normalized_fields if field["name"] not in {"_location", "_budget"}]
    return [system_entries["_location"], system_entries["_budget"], *regular_fields]
    

def _system_field_entries_from_item(item: dict[str, Any], normalized_fields: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    system = _extract_system_policies(item)
    existing = {field["name"]: field for field in normalized_fields if field["name"] in {"_location", "_budget"}}
    location_entry = existing.get("_location") or {
        "name": "_location",
        "type": "System Location",
        "description": "Ubicación de la demanda",
        "required": system["location_required"],
        "when": deepcopy(system["location_when"]),
        "validation": {},
    }
    location_entry["type"] = "System Location"
    location_entry["required"] = system["location_required"]
    location_entry["when"] = deepcopy(system["location_when"])
    location_entry["validation"] = {}

    budget_policy = system["budget_policy"]
    budget_entry = existing.get("_budget") or {
        "name": "_budget",
        "type": "System Budget",
        "description": "Presupuesto de la demanda",
        "required": budget_policy.get("required", False),
        "when": deepcopy(budget_policy.get("when", {"field": "", "operator": "equals", "value": ""})),
        "fix_or_range": budget_policy.get("fix_or_range", ""),
        "unit": budget_policy.get("unit", ""),
        "validation": {},
    }
    budget_entry["type"] = "System Budget"
    budget_entry["required"] = budget_policy.get("required", False)
    budget_entry["when"] = deepcopy(budget_policy.get("when", {"field": "", "operator": "equals", "value": ""}))
    budget_entry["fix_or_range"] = budget_policy.get("fix_or_range", "")
    budget_entry["unit"] = budget_policy.get("unit", "")
    budget_entry["validation"] = {
        "min": budget_policy.get("min", ""),
        "max": budget_policy.get("max", ""),
    }
    return {"_location": location_entry, "_budget": budget_entry}


def _extract_system_policies(item: dict[str, Any]) -> dict[str, Any]:
    location_policy = _normalize_location_policy(item.get("location_policy"))
    budget_policy = _normalize_budget_policy(item.get("budget_policy"))
    for field in item.get("fields", []) or []:
        if not isinstance(field, dict):
            continue
        name = str(field.get("name") or "").strip()
        value_type = _normalize_value_type(field.get("type") or field.get("value_type") or "")
        if name == "_location" or value_type == "system_location":
            location_policy = {
                "required": _normalize_required_mode(field.get("required")),
                "when": _normalize_required_when(field.get("when")),
            }
        if name == "_budget" or value_type == "system_budget":
            validation = field.get("validation") or {}
            budget_policy = _normalize_budget_policy(
                {
                    "required": field.get("required"),
                    "when": field.get("when"),
                    "fix_or_range": field.get("fix_or_range"),
                    "unit": field.get("unit"),
                    "min": validation.get("min"),
                    "max": validation.get("max"),
                }
            )
    return {
        "location_required": location_policy["required"],
        "location_when": location_policy["when"],
        "budget_policy": budget_policy,
    }


def _normalize_field_entry(raw_field: Any) -> dict[str, Any]:
    if not isinstance(raw_field, dict):
        raise SchemaEditorError("Cada campo del intent_type debe ser un objeto válido.")
    field_name = str(raw_field.get("name") or "").strip()
    if not field_name:
        raise SchemaEditorError("Cada campo necesita un nombre interno.")
    if "type" not in raw_field:
        raise SchemaEditorError(f"El campo '{field_name}' debe indicar su tipo.")
    value_type = _normalize_value_type(raw_field.get("type") or raw_field.get("value_type"))
    if value_type == "system_location":
        return {
            "name": "_location",
            "type": "System Location",
            "description": str(raw_field.get("description") or "Ubicación de la demanda").strip(),
            "required": _normalize_required_mode(raw_field.get("required")),
            "when": _normalize_required_when(raw_field.get("when")),
            "validation": {},
        }
    if value_type == "system_budget":
        validation = raw_field.get("validation") or {}
        return {
            "name": "_budget",
            "type": "System Budget",
            "description": str(raw_field.get("description") or "Presupuesto de la demanda").strip(),
            "required": _normalize_required_mode(raw_field.get("required")),
            "when": _normalize_required_when(raw_field.get("when")),
            "fix_or_range": _normalize_fix_or_range(raw_field.get("fix_or_range")),
            "unit": _normalize_budget_unit(raw_field.get("unit")),
            "validation": {
                "min": _normalize_number_string(validation.get("min", "")),
                "max": _normalize_number_string(validation.get("max", "")),
            },
        }
    definition = _field_entry_to_definition(raw_field)
    entry = _definition_to_field_entry(
        field_name,
        definition,
        required_mode=_normalize_required_mode(raw_field.get("required")),
        when=_normalize_required_when(raw_field.get("when")),
    )
    entry["description"] = str(raw_field.get("description") or entry["description"]).strip()
    return entry


def _normalize_field_definition(field_name: str, definition: Any) -> dict[str, Any]:
    if not isinstance(definition, dict):
        definition = {}
    value_type = _normalize_value_type(definition.get("value_type") or _infer_value_type(field_name))
    min_value = ""
    max_value = ""
    if value_type == "integer":
        min_value = _normalize_int_string(definition.get("min_value"))
        max_value = _normalize_int_string(definition.get("max_value"))
    elif value_type in {"float", "money_eur", "money_eur_range"}:
        min_value = _normalize_number_string(definition.get("min_value"))
        max_value = _normalize_number_string(definition.get("max_value"))

    min_length = ""
    max_length = ""
    if value_type == "text":
        min_length = _normalize_int_string(definition.get("min_length"))
        max_length = _normalize_int_string(definition.get("max_length"))

    min_date = ""
    max_date = ""
    if value_type in {"date", "checkin_date", "checkout_date", "date_range"}:
        min_date = _normalize_iso_date_string(definition.get("min_date"))
        max_date = _normalize_iso_date_string(definition.get("max_date"))

    min_time = ""
    max_time = ""
    if value_type == "time":
        min_time = _normalize_time_string(definition.get("min_time"))
        max_time = _normalize_time_string(definition.get("max_time"))

    min_datetime = ""
    max_datetime = ""
    if value_type == "datetime":
        min_datetime = _normalize_datetime_string(definition.get("min_datetime"))
        max_datetime = _normalize_datetime_string(definition.get("max_datetime"))

    return {
        "label_es": str(definition.get("label_es") or "").strip() or _field_label(field_name),
        "value_type": value_type,
        "choices": _normalize_choices(field_name, value_type, definition.get("choices") or []),
        "min_value": min_value,
        "max_value": max_value,
        "min_length": min_length,
        "max_length": max_length,
        "min_date": min_date,
        "max_date": max_date,
        "min_time": min_time,
        "max_time": max_time,
        "min_datetime": min_datetime,
        "max_datetime": max_datetime,
    }


def _default_field_definition(field_name: str) -> dict[str, Any]:
    value_type = _infer_value_type(field_name)
    definition = {
        "label_es": _field_label(field_name),
        "value_type": value_type,
        "choices": deepcopy(SELECT_FIELD_DEFAULTS.get(field_name, [])) if value_type == "enum" else [],
        "min_value": "1" if value_type in {"integer", "float", "money_eur"} else "",
        "max_value": "",
        "min_length": "",
        "max_length": "",
        "min_date": "",
        "max_date": "",
        "min_time": "",
        "max_time": "",
        "min_datetime": "",
        "max_datetime": "",
    }
    return definition


def _infer_value_type(field_name: str) -> str:
    lowered = field_name.lower()
    if field_name == "dates" or "date_range" in lowered or "schedule_or_dates" in lowered:
        return "date_range"
    if field_name == "checkin":
        return "checkin_date"
    if field_name == "checkout":
        return "checkout_date"
    if lowered in {"modality", "service_mode", "urgency"}:
        return "enum"
    if is_date_field(field_name) or lowered.endswith("_date") or lowered in {"date", "deadline", "move_in_date"}:
        return "date"
    if is_budget_field(field_name) or lowered.startswith("budget_") or lowered.endswith("_budget_max") or lowered.startswith("price"):
        return "money_eur"
    if is_location_field(field_name) or lowered in {"city", "country", "destination", "origin"} or lowered.endswith("_location"):
        return "location"
    if is_integer_field(field_name) or lowered in {"age", "children_count", "participants", "hours", "km", "year"}:
        return "integer"
    if is_number_field(field_name) or lowered.startswith("approx_") or lowered in {"salary", "price", "revenue", "volume", "size", "square_meters"}:
        return "float"
    if lowered.startswith(("has_", "is_")) or lowered in {"pool", "terrace", "barbecue", "pets_allowed", "flight_included", "furnished", "lift", "romantic"}:
        return "boolean"
    return "text"


def _field_label(field_name: str) -> str:
    return FIELD_LABELS.get(field_name) or field_name.replace("_", " ").strip()


def _normalize_value_type(value_type: Any) -> str:
    normalized = str(value_type or "").strip()
    normalized = TYPE_LABEL_TO_CODE.get(normalized.lower(), normalized)
    if normalized not in FIELD_VALUE_TYPES:
        raise SchemaEditorError(f"Tipo de campo no soportado: {normalized or '-'}")
    return normalized


def _normalize_choices(field_name: str, value_type: str, raw_choices: Any) -> list[dict[str, str]]:
    if value_type != "enum":
        return []
    normalized_choices: list[dict[str, str]] = []
    if isinstance(raw_choices, list):
        for item in raw_choices:
            if isinstance(item, dict):
                value = str(item.get("value") or "").strip()
                label = str(item.get("label") or value).strip()
            else:
                value = str(item or "").strip()
                label = value
            if value:
                normalized_choices.append({"value": value, "label": label or value})
    if not normalized_choices and field_name in SELECT_FIELD_DEFAULTS:
        normalized_choices = deepcopy(SELECT_FIELD_DEFAULTS[field_name])
    return normalized_choices


def _definition_to_field_entry(
    field_name: str,
    definition: dict[str, Any],
    required_mode: str,
    when: dict[str, str] | None = None,
) -> dict[str, Any]:
    value_type = _normalize_value_type(definition.get("value_type"))
    when = when or {"field": "", "operator": "equals", "value": ""}
    validation: dict[str, Any] = {}
    if value_type == "enum":
        options = [item["label"] for item in definition.get("choices") or [] if item.get("label")]
        if options:
            validation["options"] = options
            validation["allow_custom"] = True
    else:
        if definition.get("min_value"):
            if value_type == "integer":
                validation["min"] = int(str(definition["min_value"]).strip())
            else:
                validation["min"] = float(str(definition["min_value"]).replace(",", "."))
        if definition.get("max_value"):
            if value_type == "integer":
                validation["max"] = int(str(definition["max_value"]).strip())
            else:
                validation["max"] = float(str(definition["max_value"]).replace(",", "."))
        if definition.get("min_length"):
            validation["min_length"] = int(definition["min_length"])
        if definition.get("max_length"):
            validation["max_length"] = int(definition["max_length"])
        if definition.get("min_date"):
            validation["min_date"] = definition["min_date"]
        if definition.get("max_date"):
            validation["max_date"] = definition["max_date"]
        if definition.get("min_time"):
            validation["min_time"] = definition["min_time"]
        if definition.get("max_time"):
            validation["max_time"] = definition["max_time"]
        if definition.get("min_datetime"):
            validation["min_datetime"] = definition["min_datetime"]
        if definition.get("max_datetime"):
            validation["max_datetime"] = definition["max_datetime"]
    if value_type == "text" and "min_length" not in validation:
        validation["min_length"] = 1 if required_mode == "always" else 0
    if value_type == "text" and "max_length" not in validation:
        validation["max_length"] = 300
    if value_type == "checkin_date":
        validation["min_date"] = definition.get("min_date") or "today"
    if value_type == "checkout_date":
        validation["min_date"] = definition.get("min_date") or "after:checkin"
    return {
        "name": field_name,
        "type": TYPE_LABELS.get(value_type, "Texto"),
        "description": str(definition.get("label_es") or _field_label(field_name)).strip(),
        "required": _normalize_required_mode(required_mode),
        "when": when,
        "validation": validation,
    }


def _field_entry_to_definition(field: dict[str, Any]) -> dict[str, Any]:
    field_name = str(field.get("name") or "").strip()
    validation = field.get("validation") or {}
    value_type = _normalize_value_type(field.get("type") or field.get("value_type") or _infer_value_type(field_name))
    choices = []
    if value_type == "enum":
        options = validation.get("options") or []
        choices = [{"value": str(item).strip().lower(), "label": str(item).strip()} for item in options if str(item).strip()]
        if not choices and field_name in SELECT_FIELD_DEFAULTS:
            choices = deepcopy(SELECT_FIELD_DEFAULTS[field_name])
    return _normalize_field_definition(
        field_name,
        {
            "label_es": str(field.get("description") or field.get("label_es") or _field_label(field_name)).strip(),
            "value_type": value_type,
            "choices": choices,
            "min_value": validation.get("min", ""),
            "max_value": validation.get("max", ""),
            "min_length": validation.get("min_length", ""),
            "max_length": validation.get("max_length", ""),
            "min_date": validation.get("min_date", ""),
            "max_date": validation.get("max_date", ""),
            "min_time": validation.get("min_time", ""),
            "max_time": validation.get("max_time", ""),
            "min_datetime": validation.get("min_datetime", ""),
            "max_datetime": validation.get("max_datetime", ""),
        },
    )


def _field_rows_from_entries(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for item in items:
        field_name = item.get("name") or ""
        if field_name == "_location":
            output.append(
                {
                    "name": "_location",
                    "summary_name": "location",
                    "label_es": str(item.get("description") or "Ubicación de la demanda").strip(),
                    "value_type": "System Location",
                    "value_type_code": "system_location",
                    "choices_text": "",
                    "required": _normalize_required_mode(item.get("required")),
                    "when_field": str((item.get("when") or {}).get("field") or ""),
                    "when_operator": str((item.get("when") or {}).get("operator") or "equals"),
                    "when_value": _normalize_condition_value_text((item.get("when") or {}).get("value"), str((item.get("when") or {}).get("operator") or "equals")),
                    "min_value": "",
                    "max_value": "",
                    "min_length": "",
                    "max_length": "",
                    "min_date": "",
                    "max_date": "",
                    "min_time": "",
                    "max_time": "",
                    "min_datetime": "",
                    "max_datetime": "",
                    "checkout_reference": "",
                    "is_system": True,
                    "removable": False,
                    "budget_fix_or_range": "",
                    "budget_unit": "",
                }
            )
            continue
        if field_name == "_budget":
            validation = item.get("validation") or {}
            output.append(
                {
                    "name": "_budget",
                    "summary_name": "budget",
                    "label_es": str(item.get("description") or "Presupuesto de la demanda").strip(),
                    "value_type": "System Budget",
                    "value_type_code": "system_budget",
                    "choices_text": "",
                    "required": _normalize_required_mode(item.get("required")),
                    "when_field": str((item.get("when") or {}).get("field") or ""),
                    "when_operator": str((item.get("when") or {}).get("operator") or "equals"),
                    "when_value": _normalize_condition_value_text((item.get("when") or {}).get("value"), str((item.get("when") or {}).get("operator") or "equals")),
                    "min_value": str(validation.get("min", "") or ""),
                    "max_value": str(validation.get("max", "") or ""),
                    "min_length": "",
                    "max_length": "",
                    "min_date": "",
                    "max_date": "",
                    "min_time": "",
                    "max_time": "",
                    "min_datetime": "",
                    "max_datetime": "",
                    "checkout_reference": "",
                    "is_system": True,
                    "removable": False,
                    "budget_fix_or_range": str(item.get("fix_or_range") or ""),
                    "budget_unit": str(item.get("unit") or ""),
                }
            )
            continue
        definition = _field_entry_to_definition(item)
        min_date = definition.get("min_date", "")
        checkout_reference = ""
        if isinstance(min_date, str) and min_date.startswith("after:"):
            checkout_reference = min_date.split(":", 1)[1].strip()
        output.append(
            {
                "name": item["name"],
                "summary_name": item["name"],
                **definition,
                "value_type": TYPE_LABELS.get(definition.get("value_type"), "Texto"),
                "value_type_code": definition.get("value_type"),
                "choices_text": _choices_to_text(definition.get("choices") or []),
                "required": _normalize_required_mode(item.get("required")),
                "when_field": str((item.get("when") or {}).get("field") or ""),
                "when_operator": str((item.get("when") or {}).get("operator") or "equals"),
                "when_value": _normalize_condition_value_text((item.get("when") or {}).get("value"), str((item.get("when") or {}).get("operator") or "equals")),
                "checkout_reference": checkout_reference,
                "is_system": False,
                "removable": True,
                "budget_fix_or_range": "",
                "budget_unit": "",
            }
        )
    return sorted(
        output,
        key=lambda field: (
            0 if field.get("is_system") else 1,
            0 if field.get("name") == "_location" else (1 if field.get("name") == "_budget" else 2),
            0 if field.get("required") == "always" else (1 if field.get("required") == "conditional" else 2),
            str(field.get("summary_name") or field.get("name") or "").lower(),
        ),
    )


def _field_spec_to_definition_dict(spec: Any) -> dict[str, Any]:
    return {
        "label_es": getattr(spec, "description", None) or getattr(spec, "label_es", "") or _field_label(getattr(spec, "name", "")),
        "value_type": _normalize_value_type(getattr(spec, "type_label", None) or getattr(spec, "value_type", "text")),
        "choices": [dict(item) for item in getattr(spec, "choices", tuple())],
        "min_value": getattr(spec, "min_value", ""),
        "max_value": getattr(spec, "max_value", ""),
        "min_length": getattr(spec, "min_length", ""),
        "max_length": getattr(spec, "max_length", ""),
        "min_date": getattr(spec, "min_date", ""),
        "max_date": getattr(spec, "max_date", ""),
        "min_time": getattr(spec, "min_time", ""),
        "max_time": getattr(spec, "max_time", ""),
        "min_datetime": getattr(spec, "min_datetime", ""),
        "max_datetime": getattr(spec, "max_datetime", ""),
    }


def _choices_to_text(choices: list[dict[str, str]]) -> str:
    lines: list[str] = []
    for choice in choices:
        value = str(choice.get("value") or "").strip()
        label = str(choice.get("label") or value).strip()
        if not value:
            continue
        lines.append(label or value)
    return "\n".join(lines)


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


def _normalize_string_list(items: list[Any]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _normalize_number_string(value: Any) -> str:
    text = str(value or "").strip().replace(",", ".")
    if not text:
        return ""
    try:
        float(text)
    except ValueError:
        raise SchemaEditorError(f"Valor numérico inválido: {value}")
    return text


def _normalize_int_string(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return str(int(text))
    except ValueError:
        try:
            parsed = float(text.replace(",", "."))
        except ValueError:
            raise SchemaEditorError(f"Valor entero inválido: {value}")
        if not parsed.is_integer():
            raise SchemaEditorError(f"Valor entero inválido: {value}")
        return str(int(parsed))
    except TypeError:
        raise SchemaEditorError(f"Valor entero inválido: {value}")


def _normalize_iso_date_string(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text == "today":
        return text
    if text.startswith("after:") and text.split(":", 1)[1].strip():
        return text
    parts = text.split("-")
    if len(parts) != 3 or any(not part.isdigit() for part in parts):
        raise SchemaEditorError(f"Fecha inválida: {value}")
    return text


def _normalize_time_string(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) != 5 or text[2] != ":":
        raise SchemaEditorError(f"Hora inválida: {value}")
    return text


def _normalize_datetime_string(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "T" not in text:
        raise SchemaEditorError(f"Fecha-hora inválida: {value}")
    return text


def _budget_policy_display(policy: dict[str, Any]) -> str:
    required_mode = _normalize_required_mode(policy.get("required"))
    if required_mode == "never":
        return "Precio opcional"
    if required_mode == "conditional":
        when = policy.get("when") or {}
        condition_bits = [str(when.get("field") or "").strip(), str(when.get("operator") or "").strip(), str(when.get("value") or "").strip()]
        condition = " ".join(bit for bit in condition_bits if bit)
        return f"Precio condicional · {condition}".strip(" ·")
    unit = policy.get("unit") or "one-time"
    mode = "rango" if policy.get("fix_or_range") == "range" else "fijo"
    return f"Precio obligatorio · {mode} · {unit}"
