from __future__ import annotations

from typing import Any, Optional

from location_geometry import bbox_from_geojson, haversine_km


RADIUS_OPTIONS = [
    {"value": "1", "label": "1 km", "radius_km": 1, "radius_bucket": "1km"},
    {"value": "2", "label": "2 km", "radius_km": 2, "radius_bucket": "2km"},
    {"value": "5", "label": "5 km", "radius_km": 5, "radius_bucket": "5km"},
    {"value": "10", "label": "10 km", "radius_km": 10, "radius_bucket": "10km"},
    {"value": "30", "label": "30 km", "radius_km": 30, "radius_bucket": "30km"},
    {"value": "50", "label": "50 km", "radius_km": 50, "radius_bucket": "50km"},
    {"value": "100", "label": "100 km", "radius_km": 100, "radius_bucket": "100km"},
    {"value": "200", "label": "200 km", "radius_km": 200, "radius_bucket": "200km"},
    {"value": "200_plus", "label": "+200 km", "radius_km": None, "radius_bucket": "200_plus"},
]


RADIUS_BY_VALUE = {item["value"]: item for item in RADIUS_OPTIONS}
RADIUS_BY_BUCKET = {item["radius_bucket"]: item for item in RADIUS_OPTIONS}


def default_zone_payload() -> dict[str, Any]:
    option = RADIUS_BY_VALUE["10"]
    return {
        "mode": "radius_from_point",
        "label": "",
        "center": {"lat": None, "lon": None},
        "radius_km": option["radius_km"],
        "radius_bucket": option["radius_bucket"],
        "source": "",
        "raw_query": "",
        "admin_level": "",
        "bbox": None,
        "geojson": None,
    }


def normalize_zone_payload(payload: Any) -> Optional[dict[str, Any]]:
    if not isinstance(payload, dict):
        return None

    raw_mode = str(payload.get("mode") or "").strip() or "radius_from_point"
    label = str(payload.get("label") or "").strip() or "Punto seleccionado en el mapa"
    source = str(payload.get("source") or "map_click").strip() or "map_click"
    raw_query = str(payload.get("raw_query") or "").strip()
    admin_level = str(payload.get("admin_level") or payload.get("place_rank_label") or "").strip()
    geojson = payload.get("geojson")
    bbox = payload.get("bbox")
    normalized_bbox = None
    if isinstance(bbox, list) and len(bbox) == 4:
        try:
            normalized_bbox = [round(float(bbox[0]), 6), round(float(bbox[1]), 6), round(float(bbox[2]), 6), round(float(bbox[3]), 6)]
        except (TypeError, ValueError):
            normalized_bbox = None
    elif isinstance(geojson, dict):
        normalized_bbox = bbox_from_geojson(geojson)

    center = payload.get("center") or {}
    lat = lon = None
    try:
        lat = float(center.get("lat"))
        lon = float(center.get("lon"))
    except (TypeError, ValueError):
        if normalized_bbox:
            lon = round((normalized_bbox[0] + normalized_bbox[2]) / 2.0, 6)
            lat = round((normalized_bbox[1] + normalized_bbox[3]) / 2.0, 6)

    radius_bucket = str(payload.get("radius_bucket") or "").strip() or None
    radius_value = payload.get("radius_km")
    option = None
    if radius_bucket:
        option = RADIUS_BY_BUCKET.get(radius_bucket)
    if option is None and radius_value is not None:
        try:
            option = RADIUS_BY_VALUE.get(str(int(float(radius_value))))
        except (TypeError, ValueError):
            option = None
    if option is None:
        option = RADIUS_BY_VALUE["10"]

    if raw_mode == "area" and ((isinstance(geojson, dict) and geojson) or normalized_bbox or (lat is not None and lon is not None)):
        option = _approximate_area_radius_option(
            {
                "mode": "area",
                "center": {
                    "lat": round(lat, 6) if lat is not None else None,
                    "lon": round(lon, 6) if lon is not None else None,
                },
                "radius_km": option["radius_km"],
                "radius_bucket": option["radius_bucket"],
                "bbox": normalized_bbox,
                "geojson": geojson if isinstance(geojson, dict) else None,
            }
        )

    if lat is None or lon is None:
        return None

    return {
        "mode": "radius_from_point",
        "label": label,
        "center": {"lat": round(lat, 6), "lon": round(lon, 6)},
        "radius_km": option["radius_km"],
        "radius_bucket": option["radius_bucket"],
        "source": source,
        "raw_query": raw_query,
        "admin_level": admin_level,
        "bbox": None,
        "geojson": None,
    }


def compact_zone_for_transport(payload: Any) -> Optional[dict[str, Any]]:
    normalized = normalize_zone_payload(payload)
    if not normalized:
        return None
    compact = dict(normalized)
    compact["geojson"] = None
    compact["bbox"] = None

    if compact.get("mode") != "area":
        return compact

    center = compact.get("center") or {}
    lat = center.get("lat")
    lon = center.get("lon")
    if lat is None or lon is None:
        return compact

    radius_option = _approximate_area_radius_option(normalized)
    compact["radius_km"] = radius_option["radius_km"]
    compact["radius_bucket"] = radius_option["radius_bucket"]
    return compact


def _approximate_area_radius_option(payload: dict[str, Any]) -> dict[str, Any]:
    center = payload.get("center") or {}
    try:
        lat = float(center.get("lat"))
        lon = float(center.get("lon"))
    except (TypeError, ValueError):
        return RADIUS_BY_VALUE["10"]

    bbox = payload.get("bbox")
    estimated_radius_km = None
    if isinstance(bbox, list) and len(bbox) == 4:
        try:
            corners = [
                (float(bbox[1]), float(bbox[0])),
                (float(bbox[1]), float(bbox[2])),
                (float(bbox[3]), float(bbox[0])),
                (float(bbox[3]), float(bbox[2])),
            ]
            estimated_radius_km = max(
                haversine_km(lat, lon, corner_lat, corner_lon) for corner_lat, corner_lon in corners
            )
        except (TypeError, ValueError):
            estimated_radius_km = None

    if estimated_radius_km is None:
        radius_bucket = str(payload.get("radius_bucket") or "").strip()
        if radius_bucket and radius_bucket in RADIUS_BY_BUCKET:
            return RADIUS_BY_BUCKET[radius_bucket]
        radius_value = payload.get("radius_km")
        try:
            radius_key = str(int(float(radius_value)))
            if radius_key in RADIUS_BY_VALUE:
                return RADIUS_BY_VALUE[radius_key]
        except (TypeError, ValueError):
            pass
        return RADIUS_BY_VALUE["10"]

    best_option = RADIUS_OPTIONS[0]
    best_distance = float("inf")
    for option in RADIUS_OPTIONS:
        reference = option["radius_km"] if option["radius_km"] is not None else 220
        distance = abs(estimated_radius_km - reference)
        if distance < best_distance:
            best_distance = distance
            best_option = option
    return best_option


def zone_to_storage_fields(payload: Optional[dict[str, Any]]) -> dict[str, Any]:
    if not payload:
        return {
            "location_mode": None,
            "location_label": None,
            "location_lat": None,
            "location_lon": None,
            "location_radius_km": None,
            "location_radius_bucket": None,
            "location_source": None,
            "location_raw_query": None,
            "location_admin_level": None,
            "location_bbox": [],
            "location_geojson": {},
            "location_json": {},
        }

    center = payload.get("center") or {}
    return {
        "location_mode": payload.get("mode") or None,
        "location_label": payload.get("label"),
        "location_lat": center.get("lat"),
        "location_lon": center.get("lon"),
        "location_radius_km": payload.get("radius_km"),
        "location_radius_bucket": payload.get("radius_bucket"),
        "location_source": payload.get("source"),
        "location_raw_query": payload.get("raw_query"),
        "location_admin_level": payload.get("admin_level"),
        "location_bbox": [],
        "location_geojson": {},
        "location_json": {
            **payload,
            "mode": "radius_from_point",
            "bbox": None,
            "geojson": None,
        },
    }


def zone_display_value(payload: Optional[dict[str, Any]]) -> str:
    if not payload:
        return ""
    label = str(payload.get("label") or "").strip()
    if payload.get("mode") == "area":
        return label
    bucket = payload.get("radius_bucket") or ""
    option = RADIUS_BY_BUCKET.get(bucket, {})
    radius_label = option.get("label", "")
    if label and radius_label:
        return f"{label} · {radius_label}"
    return label or radius_label


def compact_zone_label(label: Optional[str], raw_query: Optional[str] = None) -> str:
    text = str(label or "").strip()
    query = str(raw_query or "").strip()
    if query and "," not in query and not _looks_like_address(query) and not _looks_like_country(query):
        return query
    if not text:
        return query

    parts = [part.strip() for part in text.split(",") if part.strip()]
    parts = [part for part in parts if not _looks_like_postcode(part) and not _looks_like_country(part)]
    while len(parts) > 1 and _looks_like_broad_region(parts[-1]):
        parts.pop()
    if not parts:
        return text

    if parts and _looks_like_address(parts[0]):
        parts = parts[1:]
    if len(parts) > 3:
        parts = parts[-3:]

    normalized_parts: list[str] = []
    for part in parts:
        if _looks_like_region(part) and normalized_parts:
            normalized_parts.append(_normalize_region_label(part))
        else:
            normalized_parts.append(part)

    if len(normalized_parts) >= 3:
        return ", ".join(normalized_parts[:3])
    if len(normalized_parts) == 2:
        return ", ".join(normalized_parts)
    return normalized_parts[0]


def _looks_like_postcode(value: str) -> bool:
    compact = value.replace(" ", "")
    return compact.isdigit() and 4 <= len(compact) <= 8


def _looks_like_country(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized in {
        "españa",
        "espanya",
        "spain",
        "francia",
        "france",
        "italia",
        "italy",
        "portugal",
        "alemania",
        "germany",
    }


def _looks_like_region(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized.startswith(("comunidad de ", "provincia de ", "county of ", "region of ")):
        return True
    return normalized in {
        "catalunya",
        "cataluña",
        "andalucía",
        "andalucia",
        "comunidad de madrid",
        "madrid",
        "barcelona",
        "girona",
        "lleida",
        "lerida",
        "tarragona",
        "valència",
        "valencia",
        "vallès occidental",
        "valles occidental",
        "vallès oriental",
        "valles oriental",
        "baix llobregat",
        "maresme",
        "garraf",
        "osona",
    }


def _looks_like_broad_region(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized.startswith(("comunidad de ", "provincia de ", "county of ", "region of ")):
        return True
    return normalized in {
        "catalunya",
        "cataluña",
        "andalucía",
        "andalucia",
        "comunidad de madrid",
        "madrid",
        "barcelona",
        "girona",
        "lleida",
        "lerida",
        "tarragona",
        "valència",
        "valencia",
    }


def _looks_like_address(value: str) -> bool:
    normalized = value.strip().lower()
    if any(char.isdigit() for char in normalized):
        return True
    return any(
        token in normalized
        for token in (
            "carrer",
            "calle",
            "avinguda",
            "avenida",
            "av.",
            "passeig",
            "paseo",
            "plaza",
            "plaça",
            "camino",
            "road",
            "street",
            "gr ",
        )
    )


def _normalize_region_label(value: str) -> str:
    normalized = value.strip().lower()
    if "vall" in normalized:
        return "Vallès"
    if normalized in {"baix llobregat"}:
        return "Baix Llobregat"
    if normalized in {"maresme"}:
        return "Maresme"
    if normalized in {"garraf"}:
        return "Garraf"
    if normalized in {"osona"}:
        return "Osona"
    return value
