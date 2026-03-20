from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

from field_normalizers import ValidationIssue
from master_schema import MasterSchemaRegistry


INTENT_ALIAS_RULES = [
    {
        "keywords": ["camping", "bungalow en camping", "camping familiar"],
        "intent_type": "hotel_booking",
    },
    {
        "keywords": ["apartamento turístico", "apartamento turistico"],
        "intent_type": "tourist_apartment",
    },
    {
        "keywords": ["busco trabajo", "quiero trabajar", "empleo como", "trabajo como"],
        "intent_type": "employee_hiring",
    },
]

FIELD_PROMPTS = {
    "subject": {
        "question": "¿Qué materia necesitas exactamente?",
        "placeholder": "Ej.: matemáticas, física, inglés...",
        "examples": ["matemáticas", "física", "inglés"],
    },
    "subjects": {
        "question": "¿Qué asignaturas necesitas reforzar?",
        "placeholder": "Ej.: matemáticas y lengua, física y química...",
        "examples": ["matemáticas y lengua", "física y química"],
    },
    "language": {
        "question": "¿Qué idioma quieres aprender o mejorar?",
        "placeholder": "Ej.: inglés, francés, alemán...",
        "examples": ["inglés", "francés", "alemán"],
    },
    "level": {
        "question": "¿Qué nivel necesitas?",
        "placeholder": "Ej.: primaria, ESO, bachillerato, universidad...",
        "examples": ["ESO", "bachillerato", "universidad"],
    },
    "current_level": {
        "question": "¿Cuál es el nivel actual?",
        "placeholder": "Ej.: A2, B1, 2º ESO, nivel básico...",
        "examples": ["A2", "B1", "2º ESO"],
    },
    "education_stage": {
        "question": "¿En qué etapa educativa está?",
        "placeholder": "Ej.: primaria, ESO, bachillerato...",
        "examples": ["primaria", "ESO", "bachillerato"],
    },
    "exam_target": {
        "question": "¿Qué examen quieres preparar exactamente?",
        "placeholder": "Ej.: teórico de coche, práctico de conducir, B2 First...",
        "examples": ["teórico de coche", "práctico de conducir", "B2 First"],
    },
    "modality": {
        "question": "¿Qué modalidad prefieres: clases remotas o presenciales?",
        "placeholder": "Ej.: presenciales, remotas, me da igual...",
        "examples": ["presenciales", "remotas", "me da igual"],
    },
    "goal": {
        "question": "¿Qué objetivo tienes?",
        "placeholder": "Ej.: aprobar, mejorar conversación, coger soltura...",
        "examples": ["aprobar", "mejorar conversación", "coger soltura"],
    },
    "frequency": {
        "question": "¿Con qué frecuencia lo necesitas?",
        "placeholder": "Ej.: 2 días por semana, cada sábado, una vez al mes...",
        "examples": ["2 días por semana", "cada sábado", "una vez al mes"],
    },
    "appliance_type": {
        "question": "¿Qué tipo de aparato, vehículo o equipo necesitas reparar?",
        "placeholder": "Ej.: patinete eléctrico, lavadora, bicicleta, móvil...",
        "examples": ["patinete eléctrico", "lavadora", "bicicleta"],
    },
    "fault": {
        "question": "¿Qué avería o problema tiene?",
        "placeholder": "Ej.: no enciende, rueda pinchada, falla el motor, pierde agua...",
        "examples": ["no enciende", "rueda pinchada", "falla el motor"],
    },
    "problem": {
        "question": "¿Qué problema o necesidad concreta tienes?",
        "placeholder": "Ej.: no enciende, pierde agua, necesito instalarlo...",
        "examples": ["no enciende", "pierde agua", "necesito instalarlo"],
    },
    "location": {
        "question": "¿En qué ciudad o zona quieres buscarlo?",
        "placeholder": "Ej.: Barcelona, Sabadell, Sant Cugat...",
        "examples": ["Barcelona", "Sabadell", "Sant Cugat"],
    },
    "vehicle_type_or_model": {
        "question": "¿Me puedes concretar tipo o modelo de vehículo?",
        "placeholder": "Ej.: Seat Ibiza, utilitario, SUV pequeño, coche familiar...",
        "examples": ["Seat Ibiza", "SUV pequeño", "coche familiar"],
    },
    "search_location": {
        "question": "¿En qué ciudad o zona concreta quieres buscarlo?",
        "placeholder": "Ej.: Madrid, Barcelona, Sant Cugat...",
        "examples": ["Madrid", "Barcelona", "Sant Cugat"],
    },
    "city_or_area": {
        "question": "¿En qué ciudad o zona concreta lo necesitas?",
        "placeholder": "Ej.: Barcelona, Sabadell, Mirasol...",
        "examples": ["Barcelona", "Sabadell", "Mirasol"],
    },
    "urgency": {
        "question": "¿Qué nivel de urgencia tienes?",
        "placeholder": "Ej.: lo necesito ya, esta semana, en los próximos días, sin prisa...",
        "examples": ["lo necesito ya", "esta semana", "sin prisa"],
    },
    "budget_max": {
        "question": "¿Cuál es tu presupuesto máximo en euros (€)?",
        "placeholder": "Ej.: 300 €, 1200 €, 20000 €...",
        "examples": ["300 €", "1200 €", "20000 €"],
    },
    "budget_total": {
        "question": "¿Cuál es tu presupuesto total en euros (€)?",
        "placeholder": "Ej.: 600 €, 1500 €, 4000 €...",
        "examples": ["600 €", "1500 €", "4000 €"],
    },
    "location_value": {
        "question": "¿En qué ciudad o zona concreta lo necesitas?",
        "placeholder": "Ej.: Barcelona, Sant Cugat, Mirasol, Calle Aragón 120 Barcelona...",
        "examples": ["Barcelona", "Sant Cugat", "Calle Aragón 120, Barcelona"],
    },
    "checkin": {
        "question": "¿Qué fecha de entrada necesitas?",
        "placeholder": "Ej.: 12/08/2026",
        "examples": ["12/08/2026"],
    },
    "checkout": {
        "question": "¿Qué fecha de salida necesitas?",
        "placeholder": "Ej.: 18/08/2026",
        "examples": ["18/08/2026"],
    },
    "date": {
        "question": "¿Qué fecha necesitas exactamente?",
        "placeholder": "Ej.: 12/08/2026",
        "examples": ["12/08/2026"],
    },
    "dates": {
        "question": "¿Qué fechas necesitas exactamente?",
        "placeholder": "Ej.: del 12/08/2026 al 18/08/2026",
        "examples": ["del 12/08/2026 al 18/08/2026"],
    },
    "people": {
        "question": "¿Para cuántas personas es?",
        "placeholder": "Ej.: 2, 4, 6...",
        "examples": ["2", "4", "6"],
    },
    "rooms": {
        "question": "¿Cuántas habitaciones necesitas?",
        "placeholder": "Ej.: 1, 2, 3...",
        "examples": ["1", "2", "3"],
    },
    "destination": {
        "question": "¿Qué destino tienes en mente?",
        "placeholder": "Ej.: Menorca, Costa Brava, Asturias...",
        "examples": ["Menorca", "Costa Brava", "Asturias"],
    },
    "destination_area": {
        "question": "¿Qué zona concreta del destino prefieres?",
        "placeholder": "Ej.: Ciutadella, Cala Galdana, sur de Tenerife...",
        "examples": ["Ciutadella", "Cala Galdana", "sur de Tenerife"],
    },
    "property_type": {
        "question": "¿Qué tipo de alojamiento o inmueble buscas?",
        "placeholder": "Ej.: apartamento, casa rural, piso, chalet...",
        "examples": ["apartamento", "casa rural", "piso"],
    },
    "product_type": {
        "question": "¿Qué tipo de producto buscas exactamente?",
        "placeholder": "Ej.: coche pequeño, cámara réflex, sofá de 3 plazas...",
        "examples": ["coche pequeño", "cámara réflex", "sofá de 3 plazas"],
    },
    "brand": {
        "question": "¿Tienes alguna marca preferida?",
        "placeholder": "Ej.: Sony, Apple, Bosch...",
        "examples": ["Sony", "Apple", "Bosch"],
    },
    "brand_model": {
        "question": "¿Qué marca o modelo buscas?",
        "placeholder": "Ej.: Sony Alpha 7 IV, iPhone 14, Bosch Serie 6...",
        "examples": ["Sony Alpha 7 IV", "iPhone 14", "Bosch Serie 6"],
    },
    "service_type": {
        "question": "¿Qué tipo de servicio necesitas exactamente?",
        "placeholder": "Ej.: instalación, reparación, mantenimiento, asesoría...",
        "examples": ["instalación", "reparación", "mantenimiento"],
    },
    "service": {
        "question": "¿Qué servicio buscas exactamente?",
        "placeholder": "Ej.: limpieza, reforma, transporte, asesoría...",
        "examples": ["limpieza", "reforma", "transporte"],
    },
    "area": {
        "question": "¿Qué tamaño o superficie aproximada necesitas?",
        "placeholder": "Ej.: 70 m², 120 m², jardín de 40 m²...",
        "examples": ["70 m²", "120 m²", "40 m²"],
    },
    "square_meters": {
        "question": "¿Cuántos metros cuadrados aproximadamente son?",
        "placeholder": "Ej.: 60 m², 95 m², 140 m²...",
        "examples": ["60 m²", "95 m²", "140 m²"],
    },
    "deadline": {
        "question": "¿Cuál es el plazo máximo que tienes?",
        "placeholder": "Ej.: esta semana, antes del 30/04/2026, en 15 días...",
        "examples": ["esta semana", "antes del 30/04/2026", "en 15 días"],
    },
    "role": {
        "question": "¿Qué puesto o perfil buscas exactamente?",
        "placeholder": "Ej.: senior developer, comercial, administrativo...",
        "examples": ["senior developer", "comercial", "administrativo"],
    },
    "seniority": {
        "question": "¿Qué nivel de experiencia buscas?",
        "placeholder": "Ej.: junior, mid, senior...",
        "examples": ["junior", "mid", "senior"],
    },
    "stack": {
        "question": "¿Qué tecnologías o conocimientos son importantes?",
        "placeholder": "Ej.: Python y FastAPI, React, Java...",
        "examples": ["Python y FastAPI", "React", "Java"],
    },
    "job_type": {
        "question": "¿Qué tipo de trabajo buscas?",
        "placeholder": "Ej.: jornada completa, media jornada, freelance...",
        "examples": ["jornada completa", "media jornada", "freelance"],
    },
    "service_mode": {
        "question": "¿Cómo prefieres que se preste el servicio?",
        "placeholder": "Ej.: presencial, online, a domicilio...",
        "examples": ["presencial", "online", "a domicilio"],
    },
}

FIELD_LABELS = {
    "subject": "materia",
    "subjects": "asignaturas",
    "language": "idioma",
    "level": "nivel",
    "current_level": "nivel actual",
    "education_stage": "etapa educativa",
    "exam_target": "examen a preparar",
    "modality": "modalidad",
    "goal": "objetivo",
    "frequency": "frecuencia",
    "appliance_type": "tipo o modelo",
    "fault": "avería",
    "problem": "problema",
    "location": "ubicación",
    "location_value": "ubicación",
    "search_location": "ubicación de búsqueda",
    "city_or_area": "ciudad o zona",
    "vehicle_type_or_model": "tipo o modelo de vehículo",
    "urgency": "urgencia",
    "budget_max": "presupuesto máximo",
    "budget_total": "presupuesto total",
    "budget_per_hour": "presupuesto por hora",
    "budget_per_day": "presupuesto por día",
    "budget_per_night": "presupuesto por noche",
    "budget_per_person": "presupuesto por persona",
    "budget_monthly": "presupuesto mensual",
    "budget_annual": "presupuesto anual",
    "date": "fecha",
    "dates": "fechas",
    "checkin": "fecha de entrada",
    "checkout": "fecha de salida",
    "people": "personas",
    "rooms": "habitaciones",
    "destination": "destino",
    "destination_area": "zona del destino",
    "property_type": "tipo de inmueble",
    "product_type": "tipo de producto",
    "brand": "marca",
    "brand_model": "marca o modelo",
    "service_type": "tipo de servicio",
    "service": "servicio",
    "area": "superficie",
    "square_meters": "metros cuadrados",
    "deadline": "plazo máximo",
    "role": "puesto",
    "seniority": "nivel de experiencia",
    "stack": "tecnologías",
    "job_type": "tipo de trabajo",
    "service_mode": "modo del servicio",
}

CITY_TO_COUNTRY = {
    "madrid": "spain",
    "barcelona": "spain",
    "valencia": "spain",
    "sevilla": "spain",
    "bilbao": "spain",
    "malaga": "spain",
    "málaga": "spain",
    "zaragoza": "spain",
    "sant cugat": "spain",
    "mirasol": "spain",
    "mira-sol": "spain",
    "vallvidrera": "spain",
    "menorca": "spain",
    "mallorca": "spain",
    "ibiza": "spain",
    "roma": "italy",
    "rome": "italy",
    "milan": "italy",
    "milanó": "italy",
    "paris": "france",
    "lisboa": "portugal",
    "porto": "portugal",
}

COUNTRY_ALIASES = {
    "españa": "spain",
    "espana": "spain",
    "spain": "spain",
    "italia": "italy",
    "italy": "italy",
    "francia": "france",
    "france": "france",
    "portugal": "portugal",
}

LOCATION_PREFERENCE_TERMS = {
    "playa",
    "fiesta",
    "tranquilo",
    "tranquila",
    "familiar",
    "ambiente",
    "buen ambiente",
    "vistas",
    "montaña",
    "montana",
    "cerca del mar",
    "zona con",
}

FUTURE_DATE_DOMAINS = {"travel", "vacation_real_estate", "events"}
TRAVEL_LIKE_INTENTS = {"hotel_booking", "tourist_apartment", "rural_house", "holiday_package", "travel_transport"}


def resolve_alias_intent_type(raw_text: str, current_intent_type: str, registry: MasterSchemaRegistry) -> str:
    lowered = raw_text.lower()
    if registry.has_intent_type(current_intent_type) and current_intent_type != registry.fallback_schema.intent_type:
        return current_intent_type
    for rule in INTENT_ALIAS_RULES:
        if any(keyword in lowered for keyword in rule["keywords"]):
            return rule["intent_type"]
    return current_intent_type


def get_field_prompt(
    field_name: str,
    raw_text: str = "",
    intent_type: str = "",
    intent_domain: str = "",
) -> dict[str, Any]:
    prompt = FIELD_PROMPTS.get(field_name, _default_prompt_for_field(field_name))
    return _contextualize_prompt(dict(prompt), field_name, raw_text, intent_type, intent_domain)


def maybe_force_future_date(parsed_iso: str, raw_text: str, raw_value: Any, intent_domain: str, intent_type: str) -> str:
    if intent_domain not in FUTURE_DATE_DOMAINS and intent_type not in TRAVEL_LIKE_INTENTS:
        return parsed_iso
    if _contains_explicit_year(raw_text):
        return parsed_iso
    parsed = date.fromisoformat(parsed_iso)
    today = date.today()
    while parsed < today:
        parsed = date(parsed.year + 1, parsed.month, parsed.day)
    return parsed.isoformat()


def detect_country_city_mismatch(known_fields: dict[str, Any]) -> Optional[ValidationIssue]:
    country_candidates = [
        known_fields.get("location_country"),
        known_fields.get("search_country"),
        known_fields.get("country"),
        known_fields.get("search_location"),
    ]
    normalized_country = next((normalize_country_name(value) for value in country_candidates if normalize_country_name(value)), None)
    city_value = (
        known_fields.get("location_city")
        or known_fields.get("location_value")
        or known_fields.get("city_or_area")
        or known_fields.get("destination")
        or known_fields.get("search_location")
    )
    if not normalized_country or not city_value:
        return None
    city_country = CITY_TO_COUNTRY.get(_normalize_key(str(city_value)))
    if city_country and city_country != normalized_country:
        if normalized_country == "spain":
            return ValidationIssue(
                field_name="location_value",
                issue_type="country_city_mismatch",
                message="La ciudad no encaja con el país indicado.",
                question="La ciudad debe estar en España. ¿Qué ciudad o zona española te interesa?",
                raw_value=city_value,
            )
        return ValidationIssue(
            field_name="location_value",
            issue_type="country_city_mismatch",
            message="La ciudad no encaja con el país indicado.",
            question="La ciudad no coincide con el país indicado. ¿Me puedes dar una ciudad o zona coherente con esa ubicación?",
            raw_value=city_value,
        )
    return None


def infer_country_constraint(raw_text: str, known_fields: dict[str, Any]) -> Optional[str]:
    explicit_candidates = [
        known_fields.get("search_country"),
        known_fields.get("location_country"),
        known_fields.get("country"),
        known_fields.get("buyer_country"),
        known_fields.get("seller_country"),
    ]
    for candidate in explicit_candidates:
        normalized = normalize_country_name(candidate)
        if normalized:
            return normalized

    derived_candidates = [
        known_fields.get("search_location"),
        known_fields.get("location_value"),
        known_fields.get("location"),
        known_fields.get("destination"),
        known_fields.get("origin"),
    ]
    for candidate in derived_candidates:
        normalized = normalize_country_name(candidate)
        if normalized:
            return normalized

    lowered = _normalize_key(raw_text)
    for alias, normalized in COUNTRY_ALIASES.items():
        if f" {alias} " in f" {lowered} ":
            return normalized
    return None


def detect_location_preference_only(raw_value: Any) -> Optional[ValidationIssue]:
    if raw_value is None:
        return None
    lowered = str(raw_value).strip().lower()
    if not lowered:
        return None
    if any(term in lowered for term in LOCATION_PREFERENCE_TERMS):
        return ValidationIssue(
            field_name="location_value",
            issue_type="location_preference_only",
            message="Esto parece una preferencia de zona, no una ubicación concreta para buscar.",
            question="Eso me sirve como preferencia, pero necesito además una ciudad o zona concreta donde buscar. ¿Cuál sería?",
            raw_value=raw_value,
        )
    return None


def dynamic_required_fields(intent_domain: str, intent_type: str, known_fields: dict[str, Any]) -> list[str]:
    normalized_modality = _normalize_key(str(known_fields.get("modality") or ""))
    fields: list[str] = []
    if intent_domain == "jobs_talent" or intent_type in {"employee_hiring", "freelance_project", "recruiter_search"}:
        if normalized_modality in {"presencial", "hybrid", "hibrida", "hibrido"}:
            fields.append("location_value")
    return fields


def normalize_country_name(value: Any) -> Optional[str]:
    if value is None:
        return None
    return COUNTRY_ALIASES.get(_normalize_key(str(value)))


def _contains_explicit_year(value: str) -> bool:
    text = value or ""
    return any(token.isdigit() and len(token) == 4 for token in text.replace("/", " ").replace("-", " ").split())


def _normalize_key(value: str) -> str:
    return (
        value.lower()
        .strip()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )


def _humanize_field_name(field_name: str) -> str:
    return FIELD_LABELS.get(field_name, field_name.replace("_", " ").strip())


def _default_prompt_for_field(field_name: str) -> dict[str, Any]:
    label = _humanize_field_name(field_name)
    lowered = field_name.lower()
    if lowered.startswith("budget_"):
        return {
            "question": f"¿Cuál es tu {label} en euros (€)?",
            "placeholder": "Ej.: 300 €, 1200 €, 20000 €...",
            "examples": ["300 €", "1200 €", "20000 €"],
        }
    if lowered in {"date", "dates"} or lowered.endswith("_date"):
        return {
            "question": f"¿Qué {label} necesitas exactamente?",
            "placeholder": "Ej.: 12/08/2026",
            "examples": ["12/08/2026"],
        }
    if "location" in lowered or lowered in {"city", "country", "destination", "origin"}:
        return {
            "question": f"¿Qué {label} necesitas concretar?",
            "placeholder": "Ej.: Barcelona, Madrid, Valencia...",
            "examples": ["Barcelona", "Madrid", "Valencia"],
        }
    if lowered in {"people", "rooms", "hours", "participants", "children_count"}:
        return {
            "question": f"¿Cuántos {label} necesitas?",
            "placeholder": "Ej.: 2, 4, 6...",
            "examples": ["2", "4", "6"],
        }
    return {
        "question": f"¿Me puedes concretar {label}?",
        "placeholder": "Escribe aquí el dato con el mayor detalle útil posible",
        "examples": [],
    }


def _contextualize_prompt(
    prompt: dict[str, Any],
    field_name: str,
    raw_text: str,
    intent_type: str,
    intent_domain: str,
) -> dict[str, Any]:
    lowered = _normalize_key(raw_text)
    subject = _infer_subject(raw_text)

    if field_name == "appliance_type" and "patinete" in lowered:
        prompt["question"] = "¿Qué tipo o modelo de patinete necesitas reparar?"
        prompt["placeholder"] = "Ej.: Xiaomi M365, patinete eléctrico urbano, Smartgyro..."
        prompt["examples"] = ["Xiaomi M365", "patinete eléctrico urbano", "Smartgyro"]
        return prompt

    if field_name == "exam_target" and ("conducir" in lowered or "carnet" in lowered):
        prompt["question"] = "¿Qué parte del examen de conducir quieres preparar?"
        prompt["placeholder"] = "Ej.: teórico de coche, práctico de conducir, test psicotécnico..."
        prompt["examples"] = ["teórico de coche", "práctico de conducir", "teórico y test"]
        return prompt

    if field_name == "exam_target" and any(token in lowered for token in ("selectividad", "evau", "pau")):
        prompt["question"] = "¿Qué examen concreto quieres preparar?"
        prompt["placeholder"] = "Ej.: selectividad, EVAU, PAU, prueba de acceso..."
        prompt["examples"] = ["selectividad", "EVAU", "PAU"]
        return prompt

    if field_name == "modality" and ("conducir" in lowered or "carnet" in lowered):
        prompt["question"] = "¿Qué modalidad prefieres: clases remotas o presenciales?"
        prompt["placeholder"] = "Ej.: presenciales, remotas, me da igual..."
        prompt["examples"] = ["presenciales", "remotas", "me da igual"]
        return prompt

    if field_name == "modality" and intent_domain == "education":
        prompt["question"] = "¿Qué modalidad prefieres: clases presenciales, online o te da igual?"
        prompt["placeholder"] = "Ej.: presenciales, online, me da igual..."
        prompt["examples"] = ["presenciales", "online", "me da igual"]
        return prompt

    if field_name == "fault" and "patinete" in lowered:
        prompt["question"] = "¿Qué avería o problema tiene el patinete?"
        prompt["placeholder"] = "Ej.: rueda pinchada, no carga la batería, falla el freno..."
        prompt["examples"] = ["rueda pinchada", "no carga la batería", "falla el freno"]
        return prompt

    if field_name in {"location", "location_value", "search_location"} and "taller" in lowered and "patinete" in lowered:
        prompt["question"] = "¿En qué ciudad o zona quieres buscar el taller de patinetes?"
        prompt["placeholder"] = "Ej.: Barcelona, Hospitalet, Sant Cugat..."
        prompt["examples"] = ["Barcelona", "Hospitalet", "Sant Cugat"]
        return prompt

    if field_name in {"location", "location_value", "search_location"} and intent_type in {"hotel_booking", "tourist_apartment", "rural_house"}:
        prompt["question"] = "¿En qué destino o zona quieres buscar el alojamiento?"
        prompt["placeholder"] = "Ej.: Menorca, Costa Brava, Playa de Palma..."
        prompt["examples"] = ["Menorca", "Costa Brava", "Playa de Palma"]
        return prompt

    if field_name in {"location", "location_value", "search_location"} and intent_type in {"employee_hiring", "freelance_project", "recruiter_search"}:
        prompt["question"] = "¿En qué ciudad o zona te gustaría trabajar?"
        prompt["placeholder"] = "Ej.: Barcelona, Madrid, Valencia..."
        prompt["examples"] = ["Barcelona", "Madrid", "Valencia"]
        return prompt

    if field_name in {"location", "location_value", "search_location"} and intent_domain == "education":
        prompt["question"] = "¿En qué ciudad o zona te iría bien hacer las clases?"
        prompt["placeholder"] = "Ej.: Barcelona, Sabadell, Sant Cugat..."
        prompt["examples"] = ["Barcelona", "Sabadell", "Sant Cugat"]
        return prompt

    if field_name in {"location", "location_value", "search_location"}:
        if "taller" in lowered and subject:
            prompt["question"] = f"¿En qué ciudad o zona quieres buscar el taller para reparar {subject}?"
        elif subject:
            prompt["question"] = f"¿En qué ciudad o zona quieres buscar {subject}?"
        return prompt

    if field_name == "appliance_type" and subject:
        prompt["question"] = f"¿Qué tipo o modelo {_with_de(subject)} necesitas reparar?"
        return prompt

    if field_name == "fault" and subject:
        prompt["question"] = f"¿Qué avería o problema tiene {subject}?"
        return prompt

    if field_name == "vehicle_type_or_model" and "coche" in lowered:
        prompt["question"] = "¿Me puedes concretar tipo o modelo de coche?"
        return prompt

    if field_name == "goal" and intent_domain == "education":
        prompt["question"] = "¿Qué objetivo tienes con estas clases?"
        return prompt

    if field_name == "frequency" and intent_domain == "education":
        prompt["question"] = "¿Con qué frecuencia necesitas las clases?"
        return prompt

    if field_name == "budget_per_hour" and intent_domain == "education":
        prompt["question"] = "¿Cuál es tu presupuesto por hora en euros (€)?"
        prompt["placeholder"] = "Ej.: 15 €, 20 €, 30 €..."
        prompt["examples"] = ["15 €", "20 €", "30 €"]
        return prompt

    if field_name == "product_type" and "coche" in lowered:
        prompt["question"] = "¿Qué tipo de coche buscas exactamente?"
        prompt["placeholder"] = "Ej.: utilitario, SUV pequeño, coche familiar..."
        prompt["examples"] = ["utilitario", "SUV pequeño", "coche familiar"]
        return prompt

    if field_name == "budget_max" and "coche" in lowered:
        prompt["question"] = "¿Cuál es tu presupuesto máximo para el coche en euros (€)?"
        return prompt

    return prompt


def _infer_subject(raw_text: str) -> str:
    lowered = _normalize_key(raw_text)
    candidates = [
        ("patinete", "el patinete"),
        ("bicicleta", "la bicicleta"),
        ("bici", "la bici"),
        ("coche", "el coche"),
        ("moto", "la moto"),
        ("lavadora", "la lavadora"),
        ("nevera", "la nevera"),
        ("frigorifico", "el frigorífico"),
        ("frigorífico", "el frigorífico"),
        ("movil", "el móvil"),
        ("móvil", "el móvil"),
        ("ordenador", "el ordenador"),
        ("caldera", "la caldera"),
        ("aire acondicionado", "el aire acondicionado"),
    ]
    for token, label in candidates:
        if token in lowered:
            return label
    return ""


def _with_de(subject: str) -> str:
    if subject.startswith("el "):
        return f"del {subject[3:]}"
    return f"de {subject}"
