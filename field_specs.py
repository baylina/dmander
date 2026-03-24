from __future__ import annotations

BUDGET_FIELDS = {
    "budget",
    "budget_min",
    "budget_max",
    "budget_total",
    "budget_per_hour",
    "budget_per_day",
    "budget_per_night",
    "budget_per_person",
    "price_max",
    "max_price",
}

DATE_FIELDS = {
    "date",
    "dates",
    "checkin",
    "checkout",
    "start_date",
    "end_date",
    "exam_date",
    "deadline",
    "delivery_date",
}

LOCATION_FIELDS = {
    "location",
    "location_value",
    "destination",
    "destination_area",
    "city_or_area",
    "search_location",
    "origin",
}

INTEGER_FIELDS = {
    "people",
    "rooms",
    "student_age",
}

NUMBER_FIELDS = {
    "max_km",
}

COUNTRY_ONLY_VALUES = {
    "espana",
    "españa",
    "spain",
    "francia",
    "france",
    "italia",
    "italy",
    "portugal",
    "alemania",
    "germany",
    "uk",
    "reino unido",
    "united kingdom",
}

SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

ADDRESS_HINTS = (
    "calle",
    "c/",
    "carrer",
    "avenida",
    "avda",
    "paseo",
    "passeig",
    "plaza",
    "travessera",
    "rambla",
    "camino",
    "carretera",
    "portal",
    "piso",
    "escalera",
    "bajo",
    "ático",
    "atico",
)


def is_budget_field(field_name: str) -> bool:
    return str(field_name or "").strip().lower() in BUDGET_FIELDS


def is_date_field(field_name: str) -> bool:
    return str(field_name or "").strip().lower() in DATE_FIELDS


def is_location_field(field_name: str) -> bool:
    return str(field_name or "").strip().lower() in LOCATION_FIELDS


def is_integer_field(field_name: str) -> bool:
    return str(field_name or "").strip().lower() in INTEGER_FIELDS


def is_number_field(field_name: str) -> bool:
    return str(field_name or "").strip().lower() in NUMBER_FIELDS
