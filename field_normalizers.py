from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Optional

from field_specs import ADDRESS_HINTS, COUNTRY_ONLY_VALUES, SPANISH_MONTHS


@dataclass
class ValidationIssue:
    field_name: str
    issue_type: str
    message: str
    question: str
    raw_value: Any = None


@dataclass
class LocationNormalization:
    mode: str
    value: Optional[str]
    structured: dict[str, Any]
    issue: Optional[ValidationIssue] = None


def parse_money_eur(value: Any, field_name: str) -> tuple[Optional[float], Optional[ValidationIssue]]:
    if value is None or value == "":
        return None, None
    if isinstance(value, (int, float)):
        return float(value), None

    raw = str(value).strip()
    lowered = raw.lower()
    if any(token in lowered for token in ("$", "usd", "dólar", "dolar", "dólares", "dolares", "£", "libras", "gbp")):
        return None, ValidationIssue(
            field_name=field_name,
            issue_type="currency_not_eur",
            message="El presupuesto debe quedar normalizado en euros.",
            question=_budget_question(field_name, ask_eur=True),
            raw_value=value,
        )

    normalized = lowered.replace("euros", "").replace("euro", "").replace("eur", "").replace("€", "").replace(" ", "")
    multiplier = 1.0
    if normalized.endswith("k"):
        multiplier = 1000.0
        normalized = normalized[:-1]

    numeric_match = re.findall(r"\d+(?:[.,]\d+)?", normalized)
    if not numeric_match:
        return None, ValidationIssue(
            field_name=field_name,
            issue_type="invalid_money",
            message="No se ha podido interpretar el importe.",
            question=_budget_question(field_name, ask_eur=True),
            raw_value=value,
        )

    candidate = numeric_match[-1]
    candidate = candidate.replace(".", "").replace(",", ".")
    try:
        amount = float(candidate) * multiplier
    except ValueError:
        return None, ValidationIssue(
            field_name=field_name,
            issue_type="invalid_money",
            message="No se ha podido interpretar el importe.",
            question=_budget_question(field_name, ask_eur=True),
            raw_value=value,
        )
    return round(amount, 2), None


def parse_positive_int(value: Any, field_name: str) -> tuple[Optional[int], Optional[ValidationIssue]]:
    if value is None or value == "":
        return None, None
    if isinstance(value, bool):
        return None, ValidationIssue(
            field_name=field_name,
            issue_type="invalid_integer",
            message="Se esperaba un número entero positivo.",
            question=_integer_question(field_name),
            raw_value=value,
        )
    if isinstance(value, int):
        return (value, None) if value > 0 else (None, _invalid_integer_issue(field_name, value))
    raw = str(value).strip()
    if re.search(r"-\s*\d+", raw):
        return None, _invalid_integer_issue(field_name, value)
    match = re.search(r"\b\d+\b", raw)
    if not match:
        return None, _invalid_integer_issue(field_name, value)
    parsed = int(match.group(0))
    if parsed <= 0:
        return None, _invalid_integer_issue(field_name, value)
    return parsed, None


def parse_positive_number(value: Any, field_name: str) -> tuple[Optional[float], Optional[ValidationIssue]]:
    if value is None or value == "":
        return None, None
    if isinstance(value, (int, float)):
        return (float(value), None) if float(value) > 0 else (None, _invalid_number_issue(field_name, value))
    raw = str(value).strip()
    if re.search(r"-\s*\d+(?:[.,]\d+)?", raw):
        return None, _invalid_number_issue(field_name, value)
    match = re.search(r"\b\d+(?:[.,]\d+)?\b", raw)
    if not match:
        return None, _invalid_number_issue(field_name, value)
    parsed = float(match.group(0).replace(",", "."))
    if parsed <= 0:
        return None, _invalid_number_issue(field_name, value)
    return parsed, None


def parse_date_value(value: Any, field_name: str, today: Optional[date] = None) -> tuple[Optional[str], Optional[ValidationIssue]]:
    if value is None or value == "":
        return None, None
    if isinstance(value, date):
        return value.isoformat(), None

    today = today or date.today()
    raw = str(value).strip()
    lowered = raw.lower()

    relative_date = _parse_relative_date(lowered, today)
    if relative_date is not None:
        return relative_date.isoformat(), None

    if re.search(r"\bal\b|\bhasta\b", lowered) and field_name != "dates":
        return None, _invalid_date_issue(field_name, value)

    iso_match = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", raw)
    if iso_match:
        return _build_iso_date(iso_match.group(1), iso_match.group(2), iso_match.group(3), field_name, value)

    slash_match = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", raw)
    if slash_match:
        year = slash_match.group(3)
        if len(year) == 2:
            year = f"20{year}"
        return _build_iso_date(year, slash_match.group(2), slash_match.group(1), field_name, value)

    slash_no_year = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})", raw)
    if slash_no_year:
        return _build_iso_date(str(today.year), slash_no_year.group(2), slash_no_year.group(1), field_name, value)

    month_match = re.fullmatch(
        r"(\d{1,2})\s*(?:de)?\s*([a-záéíóú]+)(?:\s*(?:de)?\s*(\d{4}))?",
        lowered,
    )
    if month_match and month_match.group(2) in SPANISH_MONTHS:
        day = int(month_match.group(1))
        month = SPANISH_MONTHS[month_match.group(2)]
        year = int(month_match.group(3) or today.year)
        try:
            parsed = date(year, month, day)
        except ValueError:
            return None, _invalid_date_issue(field_name, value)
        if month_match.group(3) is None and parsed < today:
            parsed = date(year + 1, month, day)
        return parsed.isoformat(), None

    return None, _invalid_date_issue(field_name, value)


def normalize_location_value(
    raw_value: Any,
    allowed_modes: list[str],
    required: bool,
    field_name: str = "location_value",
) -> LocationNormalization:
    if raw_value is None or str(raw_value).strip() == "":
        return LocationNormalization(
            mode="unspecified",
            value=None,
            structured={},
            issue=_location_missing_issue(field_name, allowed_modes) if required else None,
        )

    raw = str(raw_value).strip()
    lowered = raw.lower()
    if lowered in {"online", "remoto"} and "online" in allowed_modes:
        return LocationNormalization(
            mode="online",
            value="online",
            structured={"raw": raw, "city": None, "area": None, "address": None, "country": None},
        )
    if lowered == "híbrido" or lowered == "hibrido":
        if "hybrid" in allowed_modes:
            return LocationNormalization(
                mode="hybrid",
                value="hybrid",
                structured={"raw": raw, "city": None, "area": None, "address": None, "country": None},
            )

    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if not parts:
        return LocationNormalization("unspecified", None, {}, _location_missing_issue(field_name, allowed_modes))

    country = None
    if _simplify_token(parts[-1]) in COUNTRY_ONLY_VALUES:
        country = parts.pop()

    if not parts and country:
        return LocationNormalization(
            mode="unspecified",
            value=None,
            structured={"raw": raw, "country": country},
            issue=_location_invalid_issue(field_name, allowed_modes, country_only=True, raw_value=raw),
        )

    address = None
    city = None
    area = None

    if any(hint in lowered for hint in ADDRESS_HINTS):
        address = raw
        city = parts[-1] if parts else None
        if len(parts) >= 2:
            area = parts[-2]
    elif len(parts) >= 2:
        city = parts[-1]
        area = parts[-2]
    else:
        city = parts[0]

    if city and _simplify_token(city) in COUNTRY_ONLY_VALUES:
        return LocationNormalization(
            mode="unspecified",
            value=None,
            structured={"raw": raw, "country": city},
            issue=_location_invalid_issue(field_name, allowed_modes, country_only=True, raw_value=raw),
        )

    target_mode = _select_location_mode(allowed_modes, address=address, area=area, city=city)
    target_value = address or city or area
    if not target_value:
        return LocationNormalization(
            mode="unspecified",
            value=None,
            structured={"raw": raw, "country": country},
            issue=_location_invalid_issue(field_name, allowed_modes, country_only=False, raw_value=raw),
        )

    if target_mode in {"city", "destination"} and not city:
        return LocationNormalization(
            mode="unspecified",
            value=None,
            structured={"raw": raw, "country": country},
            issue=_location_invalid_issue(field_name, allowed_modes, country_only=False),
        )

    return LocationNormalization(
        mode=target_mode,
        value=city or target_value,
        structured={
            "raw": raw,
            "city": city,
            "area": area,
            "address": address,
            "country": country,
        },
    )


def _build_iso_date(year: str, month: str, day: str, field_name: str, raw_value: Any) -> tuple[Optional[str], Optional[ValidationIssue]]:
    try:
        parsed = date(int(year), int(month), int(day))
    except ValueError:
        return None, _invalid_date_issue(field_name, raw_value)
    return parsed.isoformat(), None


def _select_location_mode(allowed_modes: list[str], address: Optional[str], area: Optional[str], city: Optional[str]) -> str:
    if address and "exact_address" in allowed_modes:
        return "exact_address"
    if city and "city" in allowed_modes:
        return "city"
    if area and "area" in allowed_modes:
        return "area"
    if city and "destination" in allowed_modes:
        return "destination"
    if area and "area" in allowed_modes:
        return "area"
    if "exact_address" in allowed_modes and address:
        return "exact_address"
    if "destination" in allowed_modes and city:
        return "destination"
    if allowed_modes:
        return allowed_modes[0]
    return "unspecified"


def _budget_question(field_name: str, ask_eur: bool) -> str:
    if ask_eur:
        return "¿Cuál es tu presupuesto máximo en euros (€)?"
    return f"¿Me puedes indicar {field_name}?"


def _invalid_date_issue(field_name: str, raw_value: Any) -> ValidationIssue:
    return ValidationIssue(
        field_name=field_name,
        issue_type="invalid_date",
        message="La fecha no se ha podido interpretar de forma fiable.",
        question=f"No he entendido bien la fecha de {field_name}. ¿Me la puedes dar en formato DD/MM/AAAA?",
        raw_value=raw_value,
    )


def _parse_relative_date(lowered: str, today: date) -> Optional[date]:
    normalized = (
        lowered.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .strip()
    )
    compact = f" {normalized} "
    if any(token in compact for token in (" hoy ", " esta noche ", " esta tarde ", " esta manana ", " esta mañana ")):
        return today
    if any(token in compact for token in (" manana ", " mañana ")):
        return today + timedelta(days=1)

    weekday_names = {
        "lunes": 0,
        "martes": 1,
        "miercoles": 2,
        "miércoles": 2,
        "jueves": 3,
        "viernes": 4,
        "sabado": 5,
        "sábado": 5,
        "domingo": 6,
    }
    for name, weekday in weekday_names.items():
        if re.search(rf"\b(?:el\s+)?(?:siguiente|proximo|próximo)\s+{name}\b", normalized):
            return _next_weekday(today, weekday, force_next_week=True)
        if re.search(rf"\b(?:este|esta)\s+{name}\b", normalized):
            return _next_weekday(today, weekday, force_next_week=False)
        if re.fullmatch(rf"{name}", normalized):
            return _next_weekday(today, weekday, force_next_week=False)
    return None


def _next_weekday(today: date, weekday: int, force_next_week: bool) -> date:
    days_ahead = (weekday - today.weekday()) % 7
    if force_next_week and days_ahead == 0:
        days_ahead = 7
    if not force_next_week and days_ahead == 0:
        return today
    return today + timedelta(days=days_ahead)


def _location_missing_issue(field_name: str, allowed_modes: list[str]) -> ValidationIssue:
    return ValidationIssue(
        field_name=field_name,
        issue_type="missing_location",
        message="Falta una localización utilizable.",
        question=_location_question(allowed_modes),
    )


def _location_invalid_issue(field_name: str, allowed_modes: list[str], country_only: bool, raw_value: Any = None) -> ValidationIssue:
    message = "La ubicación no es suficientemente concreta para normalizarla."
    if country_only:
        message = "Un país por sí solo no es suficiente para este tipo de demanda."
    return ValidationIssue(
        field_name=field_name,
        issue_type="invalid_location",
        message=message,
        question=_location_question(allowed_modes),
        raw_value=raw_value,
    )


def _location_question(allowed_modes: list[str]) -> str:
    if "exact_address" in allowed_modes:
        return "¿Me puedes indicar la dirección exacta o, al menos, calle y ciudad?"
    if "city" in allowed_modes or "destination" in allowed_modes:
        return "¿Me puedes indicar al menos la ciudad o zona concreta? Puedes dar más detalle si quieres."
    if "area" in allowed_modes:
        return "¿Me puedes indicar al menos la zona concreta? Si quieres, también puedes dar una dirección más precisa."
    return "¿Me puedes indicar una ubicación más concreta?"


def _integer_question(field_name: str) -> str:
    if field_name == "people":
        return "¿Cuántas personas serían? Indícame un número."
    if field_name == "rooms":
        return "¿Cuántas habitaciones necesitas? Indícame un número."
    return f"¿Me puedes indicar {field_name} con un número?"


def _invalid_integer_issue(field_name: str, raw_value: Any) -> ValidationIssue:
    return ValidationIssue(
        field_name=field_name,
        issue_type="invalid_integer",
        message="Se esperaba un número entero positivo.",
        question=_integer_question(field_name),
        raw_value=raw_value,
    )


def _invalid_number_issue(field_name: str, raw_value: Any) -> ValidationIssue:
    return ValidationIssue(
        field_name=field_name,
        issue_type="invalid_number",
        message="Se esperaba un número positivo.",
        question=f"¿Me puedes indicar {field_name} con un número?",
        raw_value=raw_value,
    )


def _simplify_token(value: str) -> str:
    return (
        value.lower()
        .strip()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )
