from __future__ import annotations

import re
from typing import Any, Optional

from field_normalizers import (
    ValidationIssue,
    normalize_location_value,
    parse_date_value,
    parse_money_eur,
    parse_positive_int,
    parse_positive_number,
)
from field_specs import (
    DATE_FIELDS,
    LOCATION_FIELDS,
    is_date_field,
    is_integer_field,
    is_number_field,
)
from intent_rules import apply_intent_rules
from master_schema import ALLOWED_LOCATION_MODES, MasterSchemaRegistry
from models import DemandResult, LLMResponse
from normalization_rules import (
    detect_country_city_mismatch,
    detect_location_preference_only,
    dynamic_required_fields,
    infer_country_constraint,
    maybe_force_future_date,
    resolve_alias_intent_type,
)
from zone_selector import normalize_zone_payload, zone_to_storage_fields


CORE_ATTRIBUTE_KEYS = {
    "intent_domain",
    "intent_type",
    "summary",
    "description",
    "location",
    "location_mode",
    "location_value",
    "budget_mode",
    "budget_min",
    "budget_max",
    "urgency",
    "dates",
    "validation_issues",
}


def _schema_budget_mode(schema, known_fields: dict[str, Any] | None = None) -> str:
    budget_required = schema.budget_required_for(known_fields)
    if budget_required and schema.budget_fix_or_range == "range":
        return "required_range"
    if budget_required and schema.budget_fix_or_range == "fix":
        return "required_fixed"
    if (not budget_required) and schema.budget_fix_or_range == "range":
        return "optional_range"
    if (not budget_required) and schema.budget_fix_or_range == "fix":
        return "optional_fixed"
    return ""


def merge_known_fields(*parts: Optional[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for part in parts:
        if not part:
            continue
        for key, value in part.items():
            cleaned = _clean_value(value)
            if cleaned is None:
                continue
            if isinstance(cleaned, dict) and isinstance(merged.get(key), dict):
                merged[key] = {**merged[key], **cleaned}
            else:
                merged[key] = cleaned
    return merged


def build_normalized_demand(
    raw_text: str,
    known_fields: dict[str, Any],
    response: LLMResponse,
    registry: MasterSchemaRegistry,
) -> DemandResult:
    resolved_intent_type = resolve_alias_intent_type(raw_text, response.intent_type, registry)
    schema = registry.resolve_intent_schema(resolved_intent_type)
    merged_known = merge_known_fields(
        known_fields,
        response.known_fields,
        response.attributes,
        {"dates": response.dates},
    )
    merged_known = _augment_inferred_known_fields(schema, raw_text, merged_known)
    description = first_present(response.description, merged_known.get("description"), raw_text) or raw_text
    summary = (response.summary or raw_text).strip()

    normalized_known, normalized_attributes, validation_issues = _normalize_fields(
        schema=schema,
        merged_known=merged_known,
        response=response,
        raw_text=raw_text,
        description=description,
        summary=summary,
    )

    location_value = first_present(normalized_known.get("location_value"), normalized_known.get("location"))
    location_mode = normalized_known.get("location_mode", "unspecified")
    zone_fields = zone_to_storage_fields(normalized_attributes.get("location_zone"))
    budget_min = _to_float(normalized_known.get("budget_min"))
    budget_max = _to_float(normalized_known.get("budget_max"))
    urgency = first_present(response.urgency, normalized_known.get("urgency"))
    dates = dict(normalized_known.get("dates") or {})

    required_missing = compute_required_missing(
        schema=schema,
        known_fields=normalized_known,
        location_value=location_value,
        budget_min=budget_min,
        budget_max=budget_max,
    )
    recommended_missing = compute_recommended_missing(
        schema=schema,
        known_fields=normalized_known,
        location_value=location_value,
        budget_min=budget_min,
        budget_max=budget_max,
    )

    active_required = set(schema.active_required_fields(normalized_known))
    blocking_fields = {
        issue.field_name
        for issue in validation_issues
        if issue.field_name in active_required | {"location_value", "budget_max", "budget_min"}
    }
    enough_information = not required_missing and not blocking_fields

    normalized_known = merge_known_fields(
        normalized_known,
        {
            "intent_domain": schema.intent_domain,
            "intent_type": schema.intent_type,
            "summary": summary,
            "description": description,
            "location_mode": location_mode,
            "location_value": location_value,
            "budget_mode": response.budget_mode or _schema_budget_mode(schema, normalized_known),
            "budget_min": budget_min,
            "budget_max": budget_max,
            "urgency": urgency,
            "dates": dates,
        },
    )

    attributes = {
        **normalized_attributes,
        "description": description,
    }
    for key, value in normalized_known.items():
        if key not in CORE_ATTRIBUTE_KEYS:
            attributes.setdefault(key, value)

    return DemandResult(
        entity_type="d",
        raw_text=raw_text,
        intent_domain=schema.intent_domain,
        intent_type=schema.intent_type,
        summary=summary,
        description=description.strip(),
        location_mode=location_mode,
        location_value=location_value,
        location_label=zone_fields["location_label"],
        location_admin_level=zone_fields["location_admin_level"],
        location_lat=zone_fields["location_lat"],
        location_lon=zone_fields["location_lon"],
        location_radius_km=zone_fields["location_radius_km"],
        location_radius_bucket=zone_fields["location_radius_bucket"],
        location_source=zone_fields["location_source"],
        location_raw_query=zone_fields["location_raw_query"],
        location_bbox=zone_fields["location_bbox"],
        location_geojson=zone_fields["location_geojson"],
        location_json=zone_fields["location_json"],
        location=location_value,
        budget_mode=response.budget_mode or _schema_budget_mode(schema),
        budget_min=budget_min,
        budget_max=budget_max,
        urgency=urgency,
        dates=dates,
        attributes=attributes,
        known_fields=normalized_known,
        required_missing_fields=required_missing,
        recommended_missing_fields=recommended_missing,
        validation_issues=[issue_to_dict(issue) for issue in validation_issues],
        next_question=response.next_question,
        enough_information=enough_information,
        confidence=response.confidence,
        schema_version=registry.version,
        needs_review=(schema.intent_type == registry.fallback_schema.intent_type),
    )


def normalize_existing_demand_record(row: dict[str, Any], registry: MasterSchemaRegistry) -> dict[str, Any]:
    existing = row.get("normalized_payload")
    if isinstance(existing, dict) and existing:
        return existing

    raw_text = row.get("original_text") or row.get("summary") or ""
    resolved_intent_type = resolve_alias_intent_type(raw_text, row.get("intent_type"), registry)
    schema = registry.resolve_intent_schema(resolved_intent_type)
    attributes = dict(row.get("attributes") or {})
    known_fields = merge_known_fields(
        attributes,
        {
            "summary": row.get("summary"),
            "description": attributes.get("description") or raw_text,
            "location": row.get("location"),
            "location_value": row.get("location"),
            "budget_min": row.get("budget_min"),
            "budget_max": row.get("budget_max"),
            "urgency": row.get("urgency"),
            "intent_type": schema.intent_type,
            "intent_domain": schema.intent_domain,
        },
    )
    known_fields = _augment_inferred_known_fields(schema, raw_text, known_fields)
    response = LLMResponse(
        intent_domain=schema.intent_domain,
        intent_type=schema.intent_type,
        confidence=0.0,
        summary=row.get("summary") or raw_text,
        description=attributes.get("description") or raw_text,
        location_mode="unspecified",
        location_value=row.get("location"),
        budget_mode=_schema_budget_mode(schema, known_fields),
        budget_min=_to_float(row.get("budget_min")),
        budget_max=_to_float(row.get("budget_max")),
        urgency=row.get("urgency"),
        dates={},
        attributes=attributes,
        known_fields=known_fields,
        suggested_fields=[],
        required_missing_fields=[],
        recommended_missing_fields=[],
        missing_fields=[],
        next_question=None,
        enough_information=True,
    )
    return build_normalized_demand(raw_text, known_fields, response, registry).model_dump(mode="json")


def compute_required_missing(
    schema,
    known_fields: dict[str, Any],
    location_value: Optional[str],
    budget_min: Optional[float],
    budget_max: Optional[float],
) -> list[str]:
    dynamic_required = dynamic_required_fields(schema.intent_domain, schema.intent_type, known_fields)
    missing = [
        field
        for field in [*schema.active_required_fields(known_fields), *schema.conditional_dependency_fields(known_fields), *dynamic_required]
        if not _field_is_present(field, known_fields, location_value, budget_min, budget_max)
    ]
    if schema.location_required_for(known_fields) and not location_value:
        if "location_value" not in missing and "location" not in missing:
            missing.append("location_value")
    if schema.budget_required_for(known_fields) and schema.budget_fix_or_range == "range":
        if budget_min is None and "budget_min" not in missing:
            missing.append("budget_min")
        if budget_max is None and "budget_max" not in missing:
            missing.append("budget_max")
    elif schema.budget_required_for(known_fields) and schema.budget_fix_or_range == "fix" and budget_max is None:
        if "budget_max" not in missing:
            missing.append("budget_max")
    return _dedupe_preserving_order(missing)


def _augment_inferred_known_fields(schema, raw_text: str, known_fields: dict[str, Any]) -> dict[str, Any]:
    inferred = dict(known_fields)
    for field_name in list(schema.active_required_fields(inferred)):
        if _clean_value(inferred.get(field_name)) is not None:
            continue
        if is_date_field(field_name):
            parsed, issue = parse_date_value(raw_text, field_name)
            if parsed and not issue:
                inferred[field_name] = parsed
                continue
        if field_name == "people":
            people = _infer_people_from_text(raw_text)
            if people is not None:
                inferred[field_name] = people
    _apply_budget_inference_from_text(raw_text, inferred)
    return inferred


def _apply_budget_inference_from_text(raw_text: str, inferred: dict[str, Any]) -> None:
    if _clean_value(inferred.get("budget_max")) is None:
        max_budget = _infer_budget_limit_from_text(raw_text)
        if max_budget is not None:
            inferred["budget_max"] = max_budget


def _apply_modality_inference_from_context(schema, raw_text: str, normalized_known: dict[str, Any]) -> None:
    if _clean_value(normalized_known.get("modality")) is not None:
        return
    if not any(field.name == "modality" for field in schema.fields):
        return

    lowered = (
        str(raw_text or "")
        .lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )

    if schema.intent_domain != "education" and schema.intent_type not in {
        "language_tutoring",
        "math_tutoring",
        "school_support",
        "music_training",
        "exam_prep",
    }:
        return

    if any(token in lowered for token in ("online", "on line", "a distancia", "por zoom", "videollamada", "virtual", "remota", "remoto")):
        normalized_known["modality"] = "online"
        return

    if any(token in lowered for token in ("presencial", "en persona", "a domicilio")):
        normalized_known["modality"] = "presencial"
        return

    location_hint = first_present(
        normalized_known.get("location_value"),
        normalized_known.get("location"),
        normalized_known.get("city_or_area"),
        normalized_known.get("search_location"),
        normalized_known.get("location_city"),
        normalized_known.get("location_area"),
    )
    lesson_hint = any(
        token in lowered
        for token in ("clases", "clase particular", "clases particulares", "profesor", "profesora", "academia", "refuerzo")
    )
    if location_hint and lesson_hint:
        normalized_known["modality"] = "presencial"


def _infer_budget_limit_from_text(raw_text: str) -> Optional[float]:
    text = str(raw_text or "").strip().lower()
    if not text:
        return None
    patterns = [
        r"(?:por\s+menos\s+de|menos\s+de|máximo\s+de|maximo\s+de|hasta|tope\s+de)\s*(\d+(?:[.,]\d+)?)\s*(?:€|euros?|eur)?",
        r"(\d+(?:[.,]\d+)?)\s*(?:€|euros?|eur)\s*(?:o\s+menos|máximo|maximo|como\s+máximo|de\s+tope)?",
        r"<\s*(\d+(?:[.,]\d+)?)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        raw = match.group(1).replace(",", ".")
        try:
            value = float(raw)
        except ValueError:
            continue
        if value > 0:
            return round(value, 2)
    return None


def _infer_people_from_text(raw_text: str) -> Optional[int]:
    lowered = (
        raw_text.lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )
    if any(token in lowered for token in ("pareja", "dos personas", "2 personas", "para dos", "para 2")):
        return 2
    if any(token in lowered for token in ("una persona", "1 persona", "para uno", "para 1", "yo solo", "yo sola")):
        return 1
    text_numbers = {
        "tres": 3,
        "cuatro": 4,
        "cinco": 5,
        "seis": 6,
        "siete": 7,
        "ocho": 8,
        "nueve": 9,
        "diez": 10,
    }
    for token, value in text_numbers.items():
        if f"para {token}" in lowered or f"{token} personas" in lowered:
            return value
    import re

    match = re.search(r"\bpara\s+(\d{1,2})\b", lowered) or re.search(r"\b(\d{1,2})\s+personas?\b", lowered)
    if match:
        try:
            parsed = int(match.group(1))
            return parsed if parsed > 0 else None
        except ValueError:
            return None
    return None


def compute_recommended_missing(
    schema,
    known_fields: dict[str, Any],
    location_value: Optional[str],
    budget_min: Optional[float],
    budget_max: Optional[float],
) -> list[str]:
    missing = [
        field
        for field in schema.visible_optional_fields(known_fields)
        if not _field_is_present(field, known_fields, location_value, budget_min, budget_max)
    ]
    if (not schema.budget_required_for(known_fields)) and schema.budget_fix_or_range == "range":
        if budget_min is None:
            missing.append("budget_min")
        if budget_max is None:
            missing.append("budget_max")
    elif (not schema.budget_required_for(known_fields)) and schema.budget_fix_or_range == "fix" and budget_max is None:
        missing.append("budget_max")
    return _dedupe_preserving_order(missing)


def first_present(*values: Any) -> Any:
    for value in values:
        cleaned = _clean_value(value)
        if cleaned is not None:
            return cleaned
    return None


def issue_to_dict(issue: ValidationIssue) -> dict[str, Any]:
    return {
        "field_name": issue.field_name,
        "issue_type": issue.issue_type,
        "message": issue.message,
        "question": issue.question,
        "raw_value": issue.raw_value,
    }


def _normalize_fields(schema, merged_known: dict[str, Any], response: LLMResponse, raw_text: str, description: str, summary: str):
    normalized_known = merge_known_fields(merged_known)
    normalized_attributes = dict(response.attributes)
    validation_issues: list[ValidationIssue] = []
    country_constraint = infer_country_constraint(raw_text, normalized_known)

    if response.budget_mode:
        budget_mode = response.budget_mode
    elif schema.budget_required_for(normalized_known) and schema.budget_fix_or_range == "range":
        budget_mode = "required_range"
    elif schema.budget_required_for(normalized_known) and schema.budget_fix_or_range == "fix":
        budget_mode = "required_fixed"
    elif schema.budget_fix_or_range == "range":
        budget_mode = "optional_range"
    elif schema.budget_fix_or_range == "fix":
        budget_mode = "optional_fixed"
    else:
        budget_mode = ""
    normalized_known["budget_mode"] = budget_mode
    normalized_known["summary"] = summary
    normalized_known["description"] = description
    if country_constraint:
        normalized_known["search_country"] = country_constraint

    for field_name, raw_value in list(normalized_known.items()):
        if field_name == "dates" and isinstance(raw_value, dict):
            normalized_dates: dict[str, Any] = {}
            for date_field, date_value in raw_value.items():
                normalized_date, issue = parse_date_value(date_value, date_field)
                if issue:
                    validation_issues.append(issue)
                elif normalized_date is not None:
                    normalized_dates[date_field] = maybe_force_future_date(
                        normalized_date,
                        raw_text,
                        date_value,
                        schema.intent_domain,
                        schema.intent_type,
                    )
            normalized_known["dates"] = normalized_dates
            continue

        if field_name in {"budget", "budget_min", "budget_max", "budget_total", "budget_per_hour", "budget_per_day", "budget_per_night", "budget_per_person", "price_max", "max_price"}:
            parsed, issue = parse_money_eur(raw_value, field_name)
            if issue:
                validation_issues.append(issue)
                normalized_known.pop(field_name, None)
            elif parsed is not None:
                normalized_known[field_name] = parsed
                normalized_attributes["budget_currency"] = "EUR"
            continue

        if is_date_field(field_name):
            parsed, issue = parse_date_value(raw_value, field_name)
            if issue:
                validation_issues.append(issue)
                normalized_known.pop(field_name, None)
            elif parsed is not None:
                normalized_known[field_name] = maybe_force_future_date(
                    parsed,
                    raw_text,
                    raw_value,
                    schema.intent_domain,
                    schema.intent_type,
                )
            continue

        if is_integer_field(field_name):
            parsed, issue = parse_positive_int(raw_value, field_name)
            if issue:
                validation_issues.append(issue)
                normalized_known.pop(field_name, None)
            elif parsed is not None:
                normalized_known[field_name] = parsed
            continue

        if is_number_field(field_name):
            parsed, issue = parse_positive_number(raw_value, field_name)
            if issue:
                validation_issues.append(issue)
                normalized_known.pop(field_name, None)
            elif parsed is not None:
                normalized_known[field_name] = parsed

    zone_payload = normalize_zone_payload(
        first_present(
            normalized_known.get("location_json"),
            normalized_attributes.get("location_zone"),
        )
    )
    if zone_payload:
        normalized_known["location_mode"] = zone_payload.get("mode", "radius_from_point")
        normalized_known["location_value"] = zone_payload["label"]
        normalized_known["location"] = zone_payload["label"]
        normalized_known["location_label"] = zone_payload["label"]
        normalized_known["location_lat"] = zone_payload["center"]["lat"]
        normalized_known["location_lon"] = zone_payload["center"]["lon"]
        normalized_known["location_radius_km"] = zone_payload["radius_km"]
        normalized_known["location_radius_bucket"] = zone_payload["radius_bucket"]
        normalized_known["location_source"] = zone_payload["source"]
        normalized_known["location_raw_query"] = zone_payload["raw_query"]
        normalized_known["location_admin_level"] = zone_payload.get("admin_level")
        normalized_known["location_bbox"] = zone_payload.get("bbox") or []
        normalized_known["location_geojson"] = zone_payload.get("geojson") or {}
        normalized_known["location_json"] = zone_payload
        normalized_attributes["location_zone"] = zone_payload
        normalized_attributes["location_structured"] = {
            "label": zone_payload["label"],
            "center": zone_payload["center"],
            "mode": zone_payload.get("mode", "radius_from_point"),
            "radius_km": zone_payload["radius_km"],
            "radius_bucket": zone_payload["radius_bucket"],
            "source": zone_payload["source"],
            "raw_query": zone_payload["raw_query"],
            "admin_level": zone_payload.get("admin_level"),
            "bbox": zone_payload.get("bbox") or [],
            "geojson": zone_payload.get("geojson") or {},
        }
    else:
        location_candidate = first_present(
            response.location_value,
            normalized_known.get("location_value"),
            normalized_known.get("location"),
            normalized_known.get("destination"),
            normalized_known.get("destination_area"),
            normalized_known.get("city_or_area"),
            normalized_known.get("search_location"),
            normalized_known.get("origin"),
        )
        preference_issue = detect_location_preference_only(location_candidate)
        if preference_issue:
            validation_issues.append(preference_issue)
            normalized_attributes.setdefault("destination_preferences", []).append(str(location_candidate))
            location_candidate = None
        location = normalize_location_value(
            raw_value=location_candidate,
            allowed_modes=list(ALLOWED_LOCATION_MODES),
            required=bool(schema.location_required_for(normalized_known)),
        )
        if location.issue:
            validation_issues.append(location.issue)
        if location.value is not None:
            normalized_known["location_value"] = location.value
            normalized_known["location_mode"] = location.mode
            normalized_known["location"] = location.value
            normalized_attributes["location_structured"] = location.structured
            if location.structured.get("city"):
                normalized_known["location_city"] = location.structured["city"]
            if location.structured.get("area"):
                normalized_known["location_area"] = location.structured["area"]
            if location.structured.get("address"):
                normalized_known["location_address"] = location.structured["address"]
            if location.structured.get("country"):
                normalized_known["location_country"] = location.structured["country"]

    country_city_issue = detect_country_city_mismatch(normalized_known)
    if country_city_issue:
        validation_issues.append(country_city_issue)
        normalized_known.pop("location_value", None)
        normalized_known.pop("location", None)
        normalized_known.pop("location_city", None)
        normalized_known.pop("location_area", None)
        normalized_known.pop("location_address", None)
        normalized_known["location_mode"] = "unspecified"
        structured = dict(normalized_attributes.get("location_structured") or {})
        if structured:
            structured.pop("city", None)
            structured.pop("area", None)
            structured.pop("address", None)
            normalized_attributes["location_structured"] = structured

    _apply_modality_inference_from_context(schema, raw_text, normalized_known)

    normalized_dates = dict(normalized_known.get("dates") or {})
    for field_name in DATE_FIELDS:
        if field_name == "dates":
            continue
        if normalized_known.get(field_name):
            normalized_dates[field_name] = normalized_known[field_name]
    normalized_known["dates"] = normalized_dates

    budget_min = _to_float(first_present(normalized_known.get("budget_min")))
    budget_max = _to_float(
        first_present(
            normalized_known.get("budget_max"),
            normalized_known.get("budget_total"),
            normalized_known.get("budget_per_hour"),
            normalized_known.get("budget_per_day"),
            normalized_known.get("budget_per_night"),
            normalized_known.get("budget_per_person"),
            normalized_known.get("price_max"),
            normalized_known.get("max_price"),
        )
    )
    if budget_min is not None:
        normalized_known["budget_min"] = budget_min
    if budget_max is not None:
        normalized_known["budget_max"] = budget_max
        normalized_attributes["budget_currency"] = "EUR"

    validation_issues.extend(
        apply_intent_rules(
            schema.intent_domain,
            schema.intent_type,
            raw_text,
            normalized_known,
            normalized_attributes,
        )
    )

    return normalized_known, normalized_attributes, _dedupe_issues(validation_issues)


def _field_is_present(
    field_name: str,
    known_fields: dict[str, Any],
    location_value: Optional[str],
    budget_min: Optional[float],
    budget_max: Optional[float],
) -> bool:
    if field_name == "budget_min":
        return budget_min is not None
    if field_name in {"budget_max", "budget_total", "budget_per_hour", "budget_per_day", "budget_per_night", "budget_per_person"}:
        return budget_max is not None
    if field_name in LOCATION_FIELDS:
        return bool(location_value)
    if field_name in DATE_FIELDS:
        if field_name == "dates":
            return bool(known_fields.get("dates"))
        return _clean_value(known_fields.get(field_name)) is not None
    return _clean_value(known_fields.get(field_name)) is not None


def _to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip().replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    if isinstance(value, list):
        cleaned = [_clean_value(item) for item in value]
        cleaned = [item for item in cleaned if item is not None]
        return cleaned or None
    if isinstance(value, dict):
        cleaned = {key: _clean_value(item) for key, item in value.items()}
        cleaned = {key: item for key, item in cleaned.items() if item is not None}
        return cleaned or None
    return value


def _dedupe_preserving_order(values: list[str]) -> list[str]:
    ordered: list[str] = []
    for item in values:
        if item not in ordered:
            ordered.append(item)
    return ordered


def _dedupe_issues(issues: list[ValidationIssue]) -> list[ValidationIssue]:
    seen: set[tuple[str, str]] = set()
    deduped: list[ValidationIssue] = []
    for issue in issues:
        key = (issue.field_name, issue.issue_type)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(issue)
    return deduped
