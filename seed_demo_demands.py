from __future__ import annotations

import argparse
import random
from typing import Any

from database import (
    create_user,
    create_web_demand,
    get_user_by_email,
    init_db,
    reindex_demand_embeddings,
)


SEED_USERS = [
    ("laura.martin.demo@dmander.local", "Laura Martin"),
    ("david.romero.demo@dmander.local", "David Romero"),
    ("cristina.santos.demo@dmander.local", "Cristina Santos"),
    ("alvaro.gil.demo@dmander.local", "Alvaro Gil"),
    ("marta.nieto.demo@dmander.local", "Marta Nieto"),
    ("sergio.pardo.demo@dmander.local", "Sergio Pardo"),
    ("nuria.vera.demo@dmander.local", "Nuria Vera"),
    ("jordi.farre.demo@dmander.local", "Jordi Farre"),
]


LOCATIONS = [
    {"label": "Madrid, Comunidad de Madrid", "display": "Madrid", "lat": 40.4168, "lon": -3.7038, "radius_km": 18},
    {"label": "Barcelona, Catalunya", "display": "Barcelona", "lat": 41.3874, "lon": 2.1686, "radius_km": 16},
    {"label": "Valencia, Comunitat Valenciana", "display": "Valencia", "lat": 39.4699, "lon": -0.3763, "radius_km": 16},
    {"label": "Sevilla, Andalucia", "display": "Sevilla", "lat": 37.3891, "lon": -5.9845, "radius_km": 15},
    {"label": "Zaragoza, Aragon", "display": "Zaragoza", "lat": 41.6488, "lon": -0.8891, "radius_km": 14},
    {"label": "Malaga, Andalucia", "display": "Malaga", "lat": 36.7213, "lon": -4.4214, "radius_km": 14},
    {"label": "Bilbao, Euskadi", "display": "Bilbao", "lat": 43.2630, "lon": -2.9350, "radius_km": 12},
    {"label": "A Coruna, Galicia", "display": "A Coruna", "lat": 43.3623, "lon": -8.4115, "radius_km": 12},
    {"label": "Valladolid, Castilla y Leon", "display": "Valladolid", "lat": 41.6523, "lon": -4.7245, "radius_km": 12},
    {"label": "Gijon, Asturias", "display": "Gijon", "lat": 43.5322, "lon": -5.6611, "radius_km": 12},
    {"label": "Santander, Cantabria", "display": "Santander", "lat": 43.4623, "lon": -3.8099, "radius_km": 11},
    {"label": "Murcia, Region de Murcia", "display": "Murcia", "lat": 37.9922, "lon": -1.1307, "radius_km": 13},
    {"label": "Alicante, Comunitat Valenciana", "display": "Alicante", "lat": 38.3452, "lon": -0.4810, "radius_km": 13},
    {"label": "Granada, Andalucia", "display": "Granada", "lat": 37.1773, "lon": -3.5986, "radius_km": 12},
    {"label": "Salamanca, Castilla y Leon", "display": "Salamanca", "lat": 40.9701, "lon": -5.6635, "radius_km": 11},
    {"label": "Tarragona, Catalunya", "display": "Tarragona", "lat": 41.1189, "lon": 1.2445, "radius_km": 11},
    {"label": "Sabadell, Valles Occidental, Catalunya", "display": "Sabadell", "lat": 41.5463, "lon": 2.1086, "radius_km": 10},
    {"label": "Sant Cugat del Valles, Valles Occidental, Catalunya", "display": "Sant Cugat del Valles", "lat": 41.4720, "lon": 2.0821, "radius_km": 10},
    {"label": "Ribes de Freser, Ripolles, Catalunya", "display": "Ribes de Freser", "lat": 42.2952, "lon": 2.1657, "radius_km": 5},
    {"label": "Leon, Castilla y Leon", "display": "Leon", "lat": 42.5987, "lon": -5.5671, "radius_km": 11},
    {"label": "Cordoba, Andalucia", "display": "Cordoba", "lat": 37.8882, "lon": -4.7794, "radius_km": 13},
    {"label": "Badajoz, Extremadura", "display": "Badajoz", "lat": 38.8794, "lon": -6.9707, "radius_km": 11},
    {"label": "Vigo, Galicia", "display": "Vigo", "lat": 42.2406, "lon": -8.7207, "radius_km": 13},
    {"label": "Pamplona, Navarra", "display": "Pamplona", "lat": 42.8125, "lon": -1.6458, "radius_km": 11},
    {"label": "Las Palmas de Gran Canaria, Canarias", "display": "Las Palmas de Gran Canaria", "lat": 28.1235, "lon": -15.4363, "radius_km": 14},
]


CATALOG = [
    {
        "summary": "Cambio de bateria para coche",
        "templates": [
            "Necesito cambio de bateria para un coche compacto en {place}.",
            "Busco taller o mecanico a domicilio para cambiar la bateria del coche en {place}.",
            "Quiero presupuesto para bateria nueva y montaje en {place}.",
        ],
        "budget": (70, 190, "service"),
        "suggestions": ["marca y modelo del vehiculo", "si necesitas servicio a domicilio"],
    },
    {
        "summary": "Reparacion de pinchazo",
        "templates": [
            "Tengo un pinchazo y necesito repararlo hoy mismo en {place}.",
            "Busco taller para reparar o sustituir rueda pinchada en {place}.",
            "Necesito ayuda con un pinchazo urgente cerca de {place}.",
        ],
        "budget": (25, 90, "service"),
        "suggestions": ["tamano del neumatico", "si el coche esta inmovilizado"],
    },
    {
        "summary": "Cambio de luna trasera",
        "templates": [
            "Necesito cambiar la luna trasera del coche en {place}.",
            "Busco taller para sustitucion de cristal trasero en {place}.",
            "Quiero presupuesto para cambio de luna rota del coche cerca de {place}.",
        ],
        "budget": (180, 520, "service"),
        "suggestions": ["modelo exacto del vehiculo", "si el seguro cubre parte del coste"],
    },
    {
        "summary": "Profesor particular de fisica",
        "templates": [
            "Busco clases particulares de fisica para 2 de bachillerato en {place}.",
            "Necesito profesor de fisica para reforzar selectividad en {place}.",
            "Quiero apoyo semanal de fisica para bachillerato en {place}.",
        ],
        "budget": (14, 35, "hour"),
        "suggestions": ["modalidad presencial u online", "frecuencia semanal"],
    },
    {
        "summary": "Profesor particular de ingles",
        "templates": [
            "Busco profesora de ingles para conversacion dos veces por semana en {place}.",
            "Necesito clases de ingles para mejorar speaking en {place}.",
            "Quiero profesor de ingles para nivel B2 en {place}.",
        ],
        "budget": (12, 30, "hour"),
        "suggestions": ["nivel actual", "objetivo concreto"],
    },
    {
        "summary": "Limpieza del hogar",
        "templates": [
            "Busco persona de confianza para limpieza de piso en {place}.",
            "Necesito servicio de limpieza semanal para casa de 3 habitaciones en {place}.",
            "Quiero ayuda con limpieza general y plancha en {place}.",
        ],
        "budget": (11, 20, "hour"),
        "suggestions": ["horario preferido", "tamano aproximado de la vivienda"],
    },
    {
        "summary": "Canguro para ninos",
        "templates": [
            "Necesito canguro para dos ninos pequenos en {place}.",
            "Busco persona responsable para cuidar a mis hijos algunas tardes en {place}.",
            "Quiero niner@ para apoyo puntual por las tardes en {place}.",
        ],
        "budget": (9, 16, "hour"),
        "suggestions": ["edades de los ninos", "horario aproximado"],
    },
    {
        "summary": "Reforma de bano",
        "templates": [
            "Quiero presupuesto para reforma completa de bano en {place}.",
            "Busco empresa para renovar bano pequeno en {place}.",
            "Necesito reforma de bano con cambio de plato de ducha en {place}.",
        ],
        "budget": (2500, 9000, "service"),
        "suggestions": ["metros aproximados", "si hace falta retirar banera"],
    },
    {
        "summary": "Pintar piso",
        "templates": [
            "Necesito pintar un piso de 80 metros en {place}.",
            "Busco pintor para paredes y techos de vivienda en {place}.",
            "Quiero presupuesto para pintar salon y habitaciones en {place}.",
        ],
        "budget": (500, 2200, "service"),
        "suggestions": ["metros aproximados", "si hay que reparar grietas"],
    },
    {
        "summary": "Mudanza local",
        "templates": [
            "Busco empresa para mudanza dentro de {place}.",
            "Necesito ayuda para trasladar muebles y cajas en {place}.",
            "Quiero presupuesto para mudanza de piso pequeno en {place}.",
        ],
        "budget": (180, 900, "service"),
        "suggestions": ["volumen aproximado", "si hay ascensor"],
    },
    {
        "summary": "Comprar lavadora",
        "templates": [
            "Quiero comprar una lavadora nueva por menos de {budget_item} euros en {place}.",
            "Busco lavadora de 8 kg economica en {place}.",
            "Necesito una lavadora nueva con entrega a domicilio en {place}.",
        ],
        "budget": (250, 620, "item"),
        "suggestions": ["capacidad en kg", "si necesitas retirada del aparato viejo"],
    },
    {
        "summary": "Nevera de segunda mano",
        "templates": [
            "Busco nevera de segunda mano que funcione bien en {place}.",
            "Quiero comprar frigorifico usado economico en {place}.",
            "Necesito una nevera pequena por menos de {budget_item} euros en {place}.",
        ],
        "budget": (80, 240, "item"),
        "suggestions": ["medidas maximas", "si necesitas transporte"],
    },
    {
        "summary": "Portatil para estudiar",
        "templates": [
            "Quiero comprar portatil para estudiar por menos de {budget_item} euros en {place}.",
            "Busco portatil sencillo para ofimatica en {place}.",
            "Necesito ordenador portatil economico para clases en {place}.",
        ],
        "budget": (250, 700, "item"),
        "suggestions": ["tamano de pantalla", "si prefieres nuevo o reacondicionado"],
    },
    {
        "summary": "Seguro de coche",
        "templates": [
            "Busco seguro de coche a buen precio para contratar desde {place}.",
            "Quiero comparar seguro a terceros para mi coche en {place}.",
            "Necesito oferta de seguro de coche con asistencia en carretera desde {place}.",
        ],
        "budget": (180, 650, "service"),
        "suggestions": ["tipo de cobertura", "antiguedad del vehiculo"],
    },
    {
        "summary": "Seguro de hogar",
        "templates": [
            "Quiero contratar seguro de hogar para piso en {place}.",
            "Busco seguro de hogar economico con cobertura de agua en {place}.",
            "Necesito comparar seguro para vivienda habitual en {place}.",
        ],
        "budget": (120, 420, "service"),
        "suggestions": ["tamano de la vivienda", "si es vivienda habitual o alquiler"],
    },
    {
        "summary": "Reserva de hotel",
        "templates": [
            "Quiero reservar hotel para 2 personas en {place} por menos de {budget_night} euros por noche.",
            "Busco hotel centrico en {place} para una escapada de fin de semana.",
            "Necesito alojamiento de hotel en {place} con presupuesto maximo de {budget_night} euros por noche.",
        ],
        "budget": (55, 150, "night"),
        "suggestions": ["fechas exactas", "numero de habitaciones"],
    },
    {
        "summary": "Restaurante japones",
        "templates": [
            "Busco restaurante japones para cenar en {place}.",
            "Quiero reservar comida japonesa para dos personas cerca de {place}.",
            "Necesito recomendacion de restaurante japones con menu en {place}.",
        ],
        "budget": (20, 55, "item"),
        "suggestions": ["fecha y hora", "si prefieres delivery o mesa"],
    },
    {
        "summary": "Fisioterapeuta",
        "templates": [
            "Busco fisioterapeuta para dolor lumbar en {place}.",
            "Necesito sesion de fisioterapia por contractura cervical en {place}.",
            "Quiero fisio de confianza para espalda y hombro en {place}.",
        ],
        "budget": (30, 70, "service"),
        "suggestions": ["si necesitas bono o sesion unica", "zona concreta de dolor"],
    },
    {
        "summary": "Dentista",
        "templates": [
            "Busco dentista para empaste en {place}.",
            "Necesito revision dental y limpieza en {place}.",
            "Quiero presupuesto para dentista por molestia en muela en {place}.",
        ],
        "budget": (35, 160, "service"),
        "suggestions": ["tipo de tratamiento", "si tienes urgencia"],
    },
    {
        "summary": "Peluqueria a domicilio",
        "templates": [
            "Busco peluquera a domicilio para corte y peinado en {place}.",
            "Necesito servicio de peluqueria en casa en {place}.",
            "Quiero peinado y maquillaje para evento en {place}.",
        ],
        "budget": (25, 90, "service"),
        "suggestions": ["fecha del servicio", "tipo de peinado"],
    },
    {
        "summary": "Adiestrador canino",
        "templates": [
            "Busco adiestrador canino para perro joven en {place}.",
            "Necesito ayuda con obediencia basica para mi perro en {place}.",
            "Quiero educador canino para corregir tirones con la correa en {place}.",
        ],
        "budget": (18, 45, "hour"),
        "suggestions": ["raza o tamano del perro", "problema principal a trabajar"],
    },
    {
        "summary": "Paseador de perros",
        "templates": [
            "Busco paseador de perros para mediodias en {place}.",
            "Necesito que paseen a mi perro de lunes a viernes en {place}.",
            "Quiero paseador de confianza para perro mediano en {place}.",
        ],
        "budget": (8, 18, "service"),
        "suggestions": ["tamano del perro", "duracion del paseo"],
    },
    {
        "summary": "Abogado laboralista",
        "templates": [
            "Busco abogado laboralista para consulta sobre despido en {place}.",
            "Necesito asesoramiento laboral por fin de contrato en {place}.",
            "Quiero hablar con abogado laboral en {place}.",
        ],
        "budget": (60, 180, "service"),
        "suggestions": ["si tienes documentacion disponible", "tipo de problema laboral"],
    },
    {
        "summary": "Fontanero urgente",
        "templates": [
            "Necesito fontanero urgente por fuga de agua en {place}.",
            "Busco fontanero para reparar cisterna y fuga en {place}.",
            "Quiero arreglo de tuberia o desague cuanto antes en {place}.",
        ],
        "budget": (50, 220, "service"),
        "suggestions": ["si hay urgencia inmediata", "zona exacta de la averia"],
    },
    {
        "summary": "Electricista",
        "templates": [
            "Busco electricista para revisar enchufes que no funcionan en {place}.",
            "Necesito electricista para cambiar cuadro o diferenciales en {place}.",
            "Quiero presupuesto para varias pequenas reparaciones electricas en {place}.",
        ],
        "budget": (45, 240, "service"),
        "suggestions": ["si es urgente", "numero aproximado de puntos a revisar"],
    },
    {
        "summary": "Montaje de muebles",
        "templates": [
            "Necesito montaje de muebles recien comprados en {place}.",
            "Busco ayuda para montar armario y escritorio en {place}.",
            "Quiero montador de muebles para piso nuevo en {place}.",
        ],
        "budget": (35, 180, "service"),
        "suggestions": ["numero de muebles", "si ya tienes instrucciones y piezas"],
    },
    {
        "summary": "Fotografo para evento",
        "templates": [
            "Busco fotografo para evento familiar en {place}.",
            "Necesito fotografo para cumpleanos infantil en {place}.",
            "Quiero presupuesto de fotografia para celebracion privada en {place}.",
        ],
        "budget": (120, 650, "service"),
        "suggestions": ["fecha y duracion del evento", "tipo de entrega de fotos"],
    },
    {
        "summary": "Clases de piano",
        "templates": [
            "Busco profesor de piano para principiante en {place}.",
            "Necesito clases de piano para nina de 10 anos en {place}.",
            "Quiero aprender piano desde cero en {place}.",
        ],
        "budget": (15, 32, "hour"),
        "suggestions": ["nivel actual", "si tienes instrumento en casa"],
    },
    {
        "summary": "Diseno de logo",
        "templates": [
            "Busco disenador para logo sencillo de negocio pequeno en {place}.",
            "Necesito diseno de logo y version para redes sociales.",
            "Quiero imagen de marca basica para proyecto nuevo en {place}.",
        ],
        "budget": (80, 450, "service"),
        "suggestions": ["estilo visual deseado", "si ya tienes nombre y colores"],
    },
    {
        "summary": "Reparacion de movil",
        "templates": [
            "Necesito reparar pantalla de movil rota en {place}.",
            "Busco servicio para cambiar bateria del iphone o android en {place}.",
            "Quiero presupuesto para arreglar movil que no carga en {place}.",
        ],
        "budget": (40, 220, "service"),
        "suggestions": ["marca y modelo del movil", "tipo exacto de averia"],
    },
    {
        "summary": "Compra de sofa",
        "templates": [
            "Quiero comprar sofa de 3 plazas por menos de {budget_item} euros en {place}.",
            "Busco sofa comodo para salon pequeno en {place}.",
            "Necesito sofa nuevo con envio a domicilio en {place}.",
        ],
        "budget": (280, 950, "item"),
        "suggestions": ["medidas maximas", "si prefieres chaise longue"],
    },
    {
        "summary": "Alquiler de furgoneta",
        "templates": [
            "Busco alquiler de furgoneta por un dia en {place}.",
            "Necesito furgoneta para traslado de muebles desde {place}.",
            "Quiero alquilar furgoneta pequena o mediana en {place}.",
        ],
        "budget": (45, 140, "day"),
        "suggestions": ["fecha concreta", "tamano aproximado de la carga"],
    },
    {
        "summary": "Community manager freelance",
        "templates": [
            "Busco community manager freelance para pequeno negocio en {place}.",
            "Necesito ayuda con redes sociales de restaurante local en {place}.",
            "Quiero presupuesto para gestion de instagram y facebook en {place}.",
        ],
        "budget": (180, 650, "month"),
        "suggestions": ["redes a gestionar", "numero de publicaciones al mes"],
    },
    {
        "summary": "Traductor jurado",
        "templates": [
            "Busco traductor jurado de ingles a espanol en {place}.",
            "Necesito traduccion jurada de documentos academicos en {place}.",
            "Quiero presupuesto para traduccion oficial urgente en {place}.",
        ],
        "budget": (45, 180, "service"),
        "suggestions": ["numero de paginas", "idiomas concretos"],
    },
    {
        "summary": "Comida a domicilio saludable",
        "templates": [
            "Busco comida a domicilio saludable para varios dias en {place}.",
            "Necesito servicio de tuppers sanos en {place}.",
            "Quiero menu semanal saludable con entrega en {place}.",
        ],
        "budget": (35, 90, "service"),
        "suggestions": ["numero de menus", "si hay alergias o preferencias"],
    },
    {
        "summary": "Instalacion de aire acondicionado",
        "templates": [
            "Necesito instalacion de aire acondicionado split en {place}.",
            "Busco tecnico para montaje de aire acondicionado en piso de {place}.",
            "Quiero presupuesto para instalar aire acondicionado antes del verano en {place}.",
        ],
        "budget": (650, 1900, "service"),
        "suggestions": ["numero de splits", "si ya tienes aparato comprado"],
    },
]


def build_zone(location: dict[str, Any]) -> dict[str, Any]:
    radius_km = int(location["radius_km"])
    radius_bucket = f"{radius_km}km" if radius_km < 200 else "200km"
    return {
        "mode": "radius_from_point",
        "label": location["label"],
        "center": {"lat": location["lat"], "lon": location["lon"]},
        "radius_km": radius_km,
        "radius_bucket": radius_bucket,
        "source": "seed",
        "raw_query": location["display"],
        "admin_level": "administrative",
        "bbox": None,
        "geojson": None,
    }


def ensure_seed_users() -> list[int]:
    user_ids: list[int] = []
    for email, full_name in SEED_USERS:
        user = get_user_by_email(email)
        if not user:
            user = create_user(email, "DemoSeed2026!", full_name)
        user_ids.append(user.id)
    return user_ids


def money_value(rng: random.Random, budget_spec: tuple[int, int, str] | None) -> tuple[float | None, str]:
    if not budget_spec:
        return None, "total"
    low, high, unit = budget_spec
    value = float(rng.randrange(low, high + 1))
    if unit in {"hour", "night", "day"}:
        value = round(value / 1.0, 2)
    return value, unit


def summarize(summary_seed: str, location_display: str, rng: random.Random) -> str:
    variants = [
        f"{summary_seed} en {location_display}",
        f"{summary_seed} cerca de {location_display}",
        summary_seed,
    ]
    text = rng.choice(variants).strip()
    return text[:1].upper() + text[1:] + "."


def vary_text(text: str, rng: random.Random) -> str:
    variants = [text, text.rstrip(".") + ".", text.rstrip(".")]
    candidate = rng.choice(variants)
    if rng.random() < 0.18:
        candidate = candidate.replace("Necesito", "Busco", 1)
    if rng.random() < 0.12:
        candidate = candidate.replace("Quiero", "Me gustaria", 1)
    if rng.random() < 0.10:
        candidate = candidate.replace(" por ", " para ", 1)
    return candidate


def render_text(template: str, location: dict[str, Any], budget_amount: float | None, rng: random.Random) -> str:
    raw_budget = int(budget_amount) if budget_amount is not None else rng.randint(50, 300)
    text = template.format(
        place=location["display"],
        budget_item=raw_budget,
        budget_night=raw_budget,
    )
    return vary_text(text, rng)


def generate_demands(count: int, seed: int, include_embeddings: bool, reindex: bool) -> None:
    rng = random.Random(seed)
    init_db()
    user_ids = ensure_seed_users()

    created = 0
    for index in range(count):
        family = rng.choice(CATALOG)
        location = rng.choice(LOCATIONS)
        budget_amount, budget_unit = money_value(rng, family.get("budget"))
        text = render_text(rng.choice(family["templates"]), location, budget_amount, rng)
        summary = summarize(family["summary"], location["display"], rng)
        suggestion_pool = list(family.get("suggestions") or [])
        suggestions = rng.sample(suggestion_pool, k=min(len(suggestion_pool), rng.randint(0, min(2, len(suggestion_pool))))) if suggestion_pool else []
        create_web_demand(
            user_id=user_ids[index % len(user_ids)],
            summary=summary,
            description=text,
            location=location["label"],
            budget_max=budget_amount,
            budget_unit=budget_unit,
            zone_filter=build_zone(location),
            suggested_missing_details=suggestions,
            include_embeddings=include_embeddings,
        )
        created += 1

    print(f"✅ Demandas demo creadas: {created}")
    print(f"👤 Usuarios demo disponibles: {len(user_ids)}")
    print("🔑 Password comun para usuarios demo: DemoSeed2026!")

    if not include_embeddings:
        print("ℹ️ Se han omitido embeddings para minimizar coste de tokens.")
    if reindex:
        updated = reindex_demand_embeddings()
        print(f"✅ Embeddings reindexados para {updated} demandas.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Genera demandas demo realistas sin usar LLM.")
    parser.add_argument("--count", type=int, default=120, help="Numero de demandas a crear.")
    parser.add_argument("--seed", type=int, default=42, help="Semilla aleatoria para resultados reproducibles.")
    parser.add_argument(
        "--with-embeddings",
        action="store_true",
        help="Genera embeddings al crear cada demanda. Por defecto se omiten para gastar menos tokens.",
    )
    parser.add_argument(
        "--reindex",
        action="store_true",
        help="Reindexa embeddings al final. Util si sembraste sin embeddings y luego quieres buscador semantico.",
    )
    args = parser.parse_args()
    generate_demands(
        count=max(1, args.count),
        seed=args.seed,
        include_embeddings=args.with_embeddings,
        reindex=args.reindex,
    )


if __name__ == "__main__":
    main()
