from __future__ import annotations

from math import asin, atan2, cos, radians, sin, sqrt
from typing import Any, Iterable


EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return EARTH_RADIUS_KM * c


def radius_limit_km(radius_km: Any, radius_bucket: Any) -> float:
    if radius_bucket == "200_plus":
        return 100000.0
    try:
        return float(radius_km or 0)
    except (TypeError, ValueError):
        return 0.0


def zone_has_geometry(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    center = payload.get("center") or {}
    return center.get("lat") is not None and center.get("lon") is not None


def zone_bbox(payload: dict[str, Any] | None) -> list[float] | None:
    if not isinstance(payload, dict):
        return None
    center = payload.get("center") or {}
    try:
        lat = float(center.get("lat"))
        lon = float(center.get("lon"))
    except (TypeError, ValueError):
        return None
    radius = radius_limit_km(payload.get("radius_km"), payload.get("radius_bucket"))
    if radius <= 0:
        return [lon, lat, lon, lat]
    return _circle_bbox(lat, lon, radius)


def zones_intersect(left: dict[str, Any] | None, right: dict[str, Any] | None) -> bool:
    if not left or not right:
        return False
    left_bbox = zone_bbox(left)
    right_bbox = zone_bbox(right)
    if left_bbox and right_bbox and not bbox_intersects(left_bbox, right_bbox):
        return False

    left_center = _zone_center(left)
    right_center = _zone_center(right)
    if not left_center or not right_center:
        return bool(left_bbox and right_bbox and bbox_intersects(left_bbox, right_bbox))
    distance = haversine_km(left_center[1], left_center[0], right_center[1], right_center[0])
    return distance <= (
        radius_limit_km(left.get("radius_km"), left.get("radius_bucket"))
        + radius_limit_km(right.get("radius_km"), right.get("radius_bucket"))
    )


def bbox_from_geojson(geojson: dict[str, Any] | None) -> list[float] | None:
    if not isinstance(geojson, dict):
        return None
    coords: list[tuple[float, float]] = []
    for polygon in _iter_polygons(geojson):
        for ring in polygon:
            for lon, lat in ring:
                coords.append((lon, lat))
    if not coords:
        return None
    lons = [lon for lon, _ in coords]
    lats = [lat for _, lat in coords]
    return [min(lons), min(lats), max(lons), max(lats)]


def bbox_intersects(left: list[float], right: list[float]) -> bool:
    return not (
        left[2] < right[0]
        or left[0] > right[2]
        or left[3] < right[1]
        or left[1] > right[3]
    )


def point_in_geojson(lat: float, lon: float, geojson: dict[str, Any]) -> bool:
    point = (lon, lat)
    for polygon in _iter_polygons(geojson):
        if _point_in_polygon(point, polygon):
            return True
    return False


def circle_intersects_geojson(lat: float, lon: float, radius_km: float, geojson: dict[str, Any]) -> bool:
    point = (lon, lat)
    for polygon in _iter_polygons(geojson):
        if _circle_intersects_polygon(point, radius_km, polygon):
            return True
    return False


def geojsons_intersect(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_polygons = _iter_polygons(left)
    right_polygons = _iter_polygons(right)
    for left_polygon in left_polygons:
        for right_polygon in right_polygons:
            if _polygons_intersect(left_polygon, right_polygon):
                return True
    return False


def _zone_center(payload: dict[str, Any] | None) -> tuple[float, float] | None:
    if not isinstance(payload, dict):
        return None
    center = payload.get("center") or {}
    try:
        return (float(center.get("lon")), float(center.get("lat")))
    except (TypeError, ValueError):
        bbox = zone_bbox(payload)
        if bbox:
            return ((bbox[0] + bbox[2]) / 2.0, (bbox[1] + bbox[3]) / 2.0)
        return None


def _circle_bbox(lat: float, lon: float, radius_km: float) -> list[float]:
    lat_delta = radius_km / 111.0
    lon_delta = radius_km / (111.0 * max(cos(radians(lat)), 0.01))
    return [lon - lon_delta, lat - lat_delta, lon + lon_delta, lat + lat_delta]


def _iter_polygons(geojson: dict[str, Any]) -> list[list[list[tuple[float, float]]]]:
    geometry_type = geojson.get("type")
    coordinates = geojson.get("coordinates")
    if geometry_type == "Polygon" and isinstance(coordinates, list):
        polygon = _normalize_polygon(coordinates)
        return [polygon] if polygon else []
    if geometry_type == "MultiPolygon" and isinstance(coordinates, list):
        items = []
        for polygon_coords in coordinates:
            polygon = _normalize_polygon(polygon_coords)
            if polygon:
                items.append(polygon)
        return items
    if geometry_type == "Feature" and isinstance(geojson.get("geometry"), dict):
        return _iter_polygons(geojson["geometry"])
    if geometry_type == "FeatureCollection" and isinstance(geojson.get("features"), list):
        items: list[list[list[tuple[float, float]]]] = []
        for feature in geojson["features"]:
            if isinstance(feature, dict):
                items.extend(_iter_polygons(feature))
        return items
    return []


def _normalize_polygon(raw_polygon: Any) -> list[list[tuple[float, float]]] | None:
    if not isinstance(raw_polygon, list):
        return None
    polygon: list[list[tuple[float, float]]] = []
    for raw_ring in raw_polygon:
        if not isinstance(raw_ring, list):
            continue
        ring: list[tuple[float, float]] = []
        for coord in raw_ring:
            if not isinstance(coord, list | tuple) or len(coord) < 2:
                continue
            try:
                ring.append((float(coord[0]), float(coord[1])))
            except (TypeError, ValueError):
                continue
        if len(ring) >= 3:
            polygon.append(ring)
    return polygon or None


def _point_in_polygon(point: tuple[float, float], polygon: list[list[tuple[float, float]]]) -> bool:
    if not polygon:
        return False
    if not _point_in_ring(point, polygon[0]):
        return False
    for hole in polygon[1:]:
        if _point_in_ring(point, hole):
            return False
    return True


def _point_in_ring(point: tuple[float, float], ring: list[tuple[float, float]]) -> bool:
    x, y = point
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i]
        xj, yj = ring[j]
        intersects = ((yi > y) != (yj > y)) and (
            x < (xj - xi) * (y - yi) / ((yj - yi) or 1e-12) + xi
        )
        if intersects:
            inside = not inside
        j = i
    return inside


def _circle_intersects_polygon(point: tuple[float, float], radius_km: float, polygon: list[list[tuple[float, float]]]) -> bool:
    if _point_in_polygon(point, polygon):
        return True
    outer_ring = polygon[0] if polygon else []
    for vertex in outer_ring:
        if haversine_km(point[1], point[0], vertex[1], vertex[0]) <= radius_km:
            return True
    for segment_start, segment_end in _iter_ring_segments(outer_ring):
        if _distance_point_to_segment_km(point, segment_start, segment_end) <= radius_km:
            return True
    return False


def _polygons_intersect(left: list[list[tuple[float, float]]], right: list[list[tuple[float, float]]]) -> bool:
    left_outer = left[0] if left else []
    right_outer = right[0] if right else []
    if not left_outer or not right_outer:
        return False
    for left_seg in _iter_ring_segments(left_outer):
        for right_seg in _iter_ring_segments(right_outer):
            if _segments_intersect(left_seg[0], left_seg[1], right_seg[0], right_seg[1]):
                return True
    if _point_in_polygon(left_outer[0], right):
        return True
    if _point_in_polygon(right_outer[0], left):
        return True
    return False


def _iter_ring_segments(ring: list[tuple[float, float]]) -> Iterable[tuple[tuple[float, float], tuple[float, float]]]:
    if len(ring) < 2:
        return []
    return [
        (ring[index], ring[(index + 1) % len(ring)])
        for index in range(len(ring))
    ]


def _distance_point_to_segment_km(
    point: tuple[float, float],
    start: tuple[float, float],
    end: tuple[float, float],
) -> float:
    origin_lat = radians((point[1] + start[1] + end[1]) / 3.0)

    def project(coord: tuple[float, float]) -> tuple[float, float]:
        x = coord[0] * 111.320 * cos(origin_lat)
        y = coord[1] * 110.574
        return x, y

    px, py = project(point)
    ax, ay = project(start)
    bx, by = project(end)
    abx = bx - ax
    aby = by - ay
    ab_len_sq = abx * abx + aby * aby
    if ab_len_sq == 0:
        return sqrt((px - ax) ** 2 + (py - ay) ** 2)
    t = max(0.0, min(1.0, ((px - ax) * abx + (py - ay) * aby) / ab_len_sq))
    closest_x = ax + t * abx
    closest_y = ay + t * aby
    return sqrt((px - closest_x) ** 2 + (py - closest_y) ** 2)


def _segments_intersect(
    a1: tuple[float, float],
    a2: tuple[float, float],
    b1: tuple[float, float],
    b2: tuple[float, float],
) -> bool:
    o1 = _orientation(a1, a2, b1)
    o2 = _orientation(a1, a2, b2)
    o3 = _orientation(b1, b2, a1)
    o4 = _orientation(b1, b2, a2)

    if o1 != o2 and o3 != o4:
        return True

    if o1 == 0 and _on_segment(a1, b1, a2):
        return True
    if o2 == 0 and _on_segment(a1, b2, a2):
        return True
    if o3 == 0 and _on_segment(b1, a1, b2):
        return True
    if o4 == 0 and _on_segment(b1, a2, b2):
        return True
    return False


def _orientation(p: tuple[float, float], q: tuple[float, float], r: tuple[float, float]) -> int:
    value = (q[1] - p[1]) * (r[0] - q[0]) - (q[0] - p[0]) * (r[1] - q[1])
    if abs(value) < 1e-12:
        return 0
    return 1 if value > 0 else 2


def _on_segment(p: tuple[float, float], q: tuple[float, float], r: tuple[float, float]) -> bool:
    return (
        min(p[0], r[0]) <= q[0] <= max(p[0], r[0])
        and min(p[1], r[1]) <= q[1] <= max(p[1], r[1])
    )
