# DMANDER

DMANDER conecta demanda con oferta. El proyecto mantiene el bot de Telegram original y ahora añade una aplicación web sencilla y profesional para publicar demandas, responder con ofertas y gestionar autenticación moderna.

## Qué incluye ahora

- Home pública con todas las `d` activas
- Conteo de `o` por cada demanda
- Registro con email y contraseña
- Login y logout con sesión web
- OAuth social preparado para Google, X, Meta, Apple y GitHub
- Panel privado para:
  - crear una `d` desde un único cuadro de texto libre
  - borrar `d` abiertas propias
  - ver las `o` recibidas en cada `d`
  - responder demandas ajenas con una `o`
- Preguntas dinámicas del agente en web cuando faltan detalles, igual que en Telegram/CLI
- Validez obligatoria de la oferta: `1h`, `2h`, `4h`, `8h`, `24h` o `48h` por defecto
- Normalización de demandas gobernada por un contrato maestro JSON
- Botón `json` de depuración para inspeccionar la demanda normalizada
- Vista interna `Admin JSON` para revisar demandas normalizadas
- Edición de demandas abiertas reutilizando el mismo pipeline de normalización

## Instalación

```bash
cd /Users/coni/tmp/dmander
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Necesitas PostgreSQL accesible en `DATABASE_URL`.

La normalización usa el contrato maestro guardado dentro del proyecto en `dmander_master_schema_v01.json`, o la ruta indicada en `DMANDER_MASTER_SCHEMA_PATH` si quieres sustituirlo.

## Modos de uso

### Web

```bash
python3 main.py --web
```

Abre [http://127.0.0.1:8000](http://127.0.0.1:8000).

### Bot de Telegram

```bash
python3 main.py
```

### CLI original

```bash
python3 main.py --cli
```

## Variables de entorno

### Obligatorias para la base actual

- `DATABASE_URL`
- un proveedor LLM configurado:
  - `LLM_PROVIDER=openai` + `OPENAI_API_KEY` o `LLM_API_KEY`
  - o `LLM_PROVIDER=ollama`
  - o `LLM_PROVIDER=lmstudio`
- `TELEGRAM_BOT_TOKEN` si usas el bot
- `DMANDER_MASTER_SCHEMA_PATH` para fijar el contrato maestro JSON activo

### Web

- `SESSION_SECRET`
- `WEB_HOST`
- `WEB_PORT`
- `DEBUG_NORMALIZATION` o `SHOW_NORMALIZED_JSON` para activar el botón `json`

### OAuth social opcional

- `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET`
- `GITHUB_CLIENT_ID` / `GITHUB_CLIENT_SECRET`
- `META_CLIENT_ID` / `META_CLIENT_SECRET`
- `X_CLIENT_ID` / `X_CLIENT_SECRET`
- `APPLE_CLIENT_ID` / `APPLE_CLIENT_SECRET`

Si un proveedor no está configurado, simplemente no aparece como opción en la pantalla de acceso.

### Modelos locales

DMANDER puede usar proveedores OpenAI-compatible para chat, embeddings y reranking.

Ejemplo con `Ollama`:

```env
LLM_PROVIDER=ollama
LLM_MODEL=qwen2.5:7b-instruct
LLM_BASE_URL=http://127.0.0.1:11434/v1
EMBEDDING_PROVIDER=ollama
EMBEDDING_MODEL=nomic-embed-text
RERANK_PROVIDER=ollama
RERANK_MODEL=qwen2.5:7b-instruct
```

Ejemplo con `LM Studio`:

```env
LLM_PROVIDER=lmstudio
LLM_MODEL=local-model
LLM_BASE_URL=http://127.0.0.1:1234/v1
EMBEDDING_PROVIDER=lmstudio
EMBEDDING_MODEL=text-embedding-nomic-embed-text-v1.5
RERANK_PROVIDER=lmstudio
RERANK_MODEL=local-model
```

Si quieres mantener OpenAI solo para embeddings:

```env
LLM_PROVIDER=ollama
LLM_MODEL=qwen2.5:7b-instruct
LLM_BASE_URL=http://127.0.0.1:11434/v1
EMBEDDING_PROVIDER=openai
OPENAI_API_KEY=...
```

## Esquema funcional

### Usuarios

- Cuenta local con contraseña cifrada usando `scrypt`
- Sesión basada en cookie firmada
- Login social enlazado por proveedor OAuth

### Demandas

- Públicas mientras estén en estado `open`
- Pueden ser eliminadas por su creador mientras sigan abiertas
- Muestran el número de ofertas recibidas
- Guardan un `normalized_payload` JSON alineado con el contrato maestro

### Ofertas

- Una oferta por ofertante y demanda
- Si el ofertante vuelve a responder, su oferta se actualiza
- Cada oferta guarda un vencimiento calculado según la validez elegida

## Normalización con contrato maestro

Cada demanda pasa por este flujo:

`texto libre -> intent_domain / intent_type -> schema del contrato -> extracción de campos -> missing required / recommended -> pregunta adicional -> demanda normalizada final`

La fuente de verdad para `intent_domain`, `intent_type`, `fields`, `location_policy` y `budget_policy` es el JSON maestro configurado en `DMANDER_MASTER_SCHEMA_PATH`.

Encima de ese contrato hay una capa de reglas ajustable en:

- [normalization_rules.py](/Users/coni/tmp/dmander/normalization_rules.py)
- [field_specs.py](/Users/coni/tmp/dmander/field_specs.py)
- [field_normalizers.py](/Users/coni/tmp/dmander/field_normalizers.py)

Ahí están centralizadas las correcciones de negocio como:

- alias de clasificación (`camping` -> alojamiento)
- presupuesto en euros
- fechas futuras para viajes
- validación país/ciudad
- diferencia entre ubicación real y preferencia de destino
- requisitos dinámicos como ubicación obligatoria si un trabajo es presencial

La representación normalizada interna incluye:

- `entity_type`
- `raw_text`
- `intent_domain`
- `intent_type`
- `summary`
- `description`
- `location_mode`
- `location_value`
- `budget_mode`
- `budget_min`
- `budget_max`
- `urgency`
- `dates`
- `attributes`
- `known_fields`
- `required_missing_fields`
- `recommended_missing_fields`
- `next_question`
- `enough_information`
- `confidence`

## Debug `json`

Si activas:

```bash
DEBUG_NORMALIZATION=true
```

o:

```bash
SHOW_NORMALIZED_JSON=true
```

la interfaz mostrará un botón pequeño `json` junto a cada demanda renderizada. Ese botón abre un modal con el JSON normalizado completo y permite copiarlo al portapapeles.

Con ese mismo modo debug activo también aparece una vista interna en:

`/admin/demands`

para inspeccionar rápidamente todas las demandas con su `normalized_payload`.

## Archivos principales

```text
dmander/
├── main.py
├── master_schema.py
├── normalization_rules.py
├── field_specs.py
├── field_normalizers.py
├── dmander_master_schema_v01.json
├── demand_normalizer.py
├── webapp.py
├── database.py
├── models.py
├── templates/
├── static/
├── bot.py
├── agent.py
└── requirements.txt
```

## Nota sobre Apple y X

Apple y X tienen configuraciones OAuth más delicadas que Google o GitHub. La aplicación ya deja el flujo preparado, pero para que funcionen en producción necesitarás registrar correctamente las URLs de callback y los secretos del proveedor.
