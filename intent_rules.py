from __future__ import annotations

from typing import Any, Callable

from field_normalizers import ValidationIssue


IntentRule = Callable[[str, dict[str, Any], dict[str, Any]], list[ValidationIssue]]


def apply_intent_rules(
    intent_domain: str,
    intent_type: str,
    raw_text: str,
    known_fields: dict[str, Any],
    attributes: dict[str, Any],
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for rule in INTENT_RULES_BY_TYPE.get(intent_type, []):
        issues.extend(rule(raw_text, known_fields, attributes))
    for rule in INTENT_RULES_BY_DOMAIN.get(intent_domain, []):
        issues.extend(rule(raw_text, known_fields, attributes))
    return issues


def _restaurant_people_rule(_: str, known_fields: dict[str, Any], __: dict[str, Any]) -> list[ValidationIssue]:
    people = known_fields.get("people")
    if people is None:
        return []
    try:
        parsed = int(people)
    except (TypeError, ValueError):
        return [
            ValidationIssue(
                field_name="people",
                issue_type="invalid_integer",
                message="El número de personas debe ser un entero positivo.",
                question="¿Para cuántas personas es la reserva?",
                raw_value=people,
            )
        ]
    if parsed <= 0:
        return [
            ValidationIssue(
                field_name="people",
                issue_type="invalid_integer",
                message="El número de personas debe ser mayor que 0.",
                question="¿Para cuántas personas es la reserva?",
                raw_value=people,
            )
        ]
    return []


INTENT_RULES_BY_TYPE: dict[str, list[IntentRule]] = {
    "restaurant_booking": [_restaurant_people_rule],
    "food_experience": [_restaurant_people_rule],
}


INTENT_RULES_BY_DOMAIN: dict[str, list[IntentRule]] = {}
