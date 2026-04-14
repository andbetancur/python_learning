# github_project — ETL GitHub → BigQuery

Framework de extracción de datos de la API de GitHub hacia BigQuery.
Proyecto de aprendizaje práctico para dominar `requests`, `pandas` e interacciones con APIs REST.

---

## Estructura del proyecto

```
github_project/
├── config.py
├── utils/
│   ├── logger.py
│   └── secrets.py
├── connectors/
│   ├── github_connector.py
│   └── bigquery_connector.py
├── models/
│   └── schemas.py
├── extractors/
│   ├── repos_extractor.py
│   ├── commits_extractor.py
│   └── pull_requests_extractor.py
├── main.py
├── requirements.txt
├── Dockerfile
├── .env.example
└── README.md
```

---

## Archivos — orden de creación y propósito

### 1. `config.py`
**Qué hace:** Centraliza todas las constantes del proyecto: ID del proyecto GCP, nombre del dataset en BigQuery, usuario de GitHub y nombres de los secretos en Secret Manager.

**Cómo se usa:** Todos los demás módulos lo importan con `import config` para leer estas constantes sin repetir strings.

**Por qué primero:** Es la base de todo. Si cambia el proyecto GCP o el dataset, se modifica en un solo lugar y el cambio se propaga automáticamente.

---

### 2. `utils/logger.py`
**Qué hace:** Define `get_logger(name)`, una función que devuelve un logger de Python configurado con formato estándar (`fecha | nivel | módulo | mensaje`) y nivel configurable via variable de entorno `LOG_LEVEL`.

**Cómo se usa:**
```python
from utils.logger import get_logger
logger = get_logger(__name__)
logger.info("Extracción iniciada")
```

**Por qué segundo:** Los connectors y extractores necesitan loggear desde el primer momento. Crearla antes que cualquier otro módulo evita tener `print()` provisionales que luego hay que reemplazar.

---

### 3. `utils/secrets.py`
**Qué hace:** Define `get_secret(project_id, secret_name)`, que consulta Google Secret Manager y devuelve el valor del secreto como string. Usa `@lru_cache` para no repetir llamadas a GCP si el mismo secreto se pide más de una vez en la misma ejecución.

**Cómo se usa:**
```python
from utils.secrets import get_secret
token = get_secret("mi-proyecto-gcp", "api_info_github")
```

**Por qué tercero:** Los connectors necesitan credenciales para autenticarse. Este módulo resuelve ese problema de forma segura — las credenciales nunca tocan el disco ni se hardcodean en el código.

---

### 4. `connectors/github_connector.py`
**Qué hace:** Encapsula toda la comunicación HTTP con la API de GitHub. Maneja autenticación por token, paginación automática, rate limit (espera hasta que se resetee el límite), y reintentos con backoff exponencial para errores de servidor.

**Cómo se usa:**
```python
from connectors.github_connector import GitHubConnector
gh = GitHubConnector(project_id=config.GCP_PROJECT_ID, secret_name=config.SECRET_GITHUB)
repos = gh.get_paginated("/user/repos")         # lista paginada
user  = gh.get("/users/andbetancur")            # objeto único
```

**Por qué cuarto:** Es la fuente de datos. Sin él los extractores no tienen forma de hablar con GitHub. Se construye antes que los extractores para que ellos solo se preocupen por la lógica de negocio, no por HTTP.

---

### 5. `connectors/bigquery_connector.py`
**Qué hace:** Encapsula toda la interacción con BigQuery. Ofrece tres operaciones:
- `load_dataframe()` — carga completa (WRITE_TRUNCATE), borra y recarga la tabla entera.
- `upsert_dataframe()` — carga incremental con MERGE SQL: actualiza filas existentes e inserta nuevas.
- `get_watermark()` — obtiene el valor máximo de una columna de fecha para saber desde dónde extraer en el siguiente incremental.

Agrega automáticamente la columna `process_date` (fecha de procesamiento en UTC) antes de cada carga.

**Cómo se usa:**
```python
from connectors.bigquery_connector import BigQueryConnector
bq = BigQueryConnector(config.GCP_PROJECT_ID, config.BQ_DATASET, config.SECRET_BQ_SA)
bq.load_dataframe(df, "repositories", REPOS_SCHEMA)
bq.upsert_dataframe(df, "commits", COMMITS_SCHEMA, merge_keys=["sha"])
watermark = bq.get_watermark("commits", "committed_at")
```

**Por qué quinto:** Es el destino de los datos. Se construye antes que los extractores para que ellos solo llamen métodos de alto nivel sin conocer los detalles de BigQuery.

---

### 6. `models/schemas.py`
**Qué hace:** Define los schemas de las tres tablas de BigQuery como listas de `bigquery.SchemaField`. Cada campo tiene nombre, tipo y un comentario explicando su origen en la API de GitHub.

| Schema | Tabla | PK | Watermark |
|---|---|---|---|
| `REPOS_SCHEMA` | `repositories` | `id` | `updated_at` |
| `COMMITS_SCHEMA` | `commits` | `sha` | `committed_at` |
| `PULL_REQUESTS_SCHEMA` | `pull_requests` | `id` | `updated_at` |

**Cómo se usa:**
```python
from models.schemas import REPOS_SCHEMA, COMMITS_SCHEMA, PULL_REQUESTS_SCHEMA
bq.load_dataframe(df, "repositories", REPOS_SCHEMA)
```

**Por qué sexto:** Los schemas son el contrato entre los extractores y BigQuery. Definirlos antes de escribir los extractores obliga a pensar qué campos necesitamos y cómo se llaman, antes de escribir el código de transformación.

---

### 7. `extractors/repos_extractor.py` _(pendiente)_
**Qué hará:** Extrae repositorios del usuario desde `GET /user/repos`, transforma el JSON a un DataFrame de pandas (aplanando campos anidados como `owner.login` y `topics`), y carga en BigQuery.

**Cómo se usará:**
```python
from extractors.repos_extractor import ReposExtractor
extractor = ReposExtractor()
extractor.run(mode="full")        # WRITE_TRUNCATE
extractor.run(mode="incremental") # MERGE desde el último updated_at
```

**Por qué en este orden:** Se crea antes que commits y PRs porque es el más simple (un solo endpoint, sin dependencias de otros extractores) y establece el patrón que los demás van a seguir.

---

### 8. `extractors/commits_extractor.py` _(pendiente)_
**Qué hará:** Para cada repo extraído, llama a `GET /repos/{owner}/{repo}/commits` y carga los resultados en BigQuery. En modo incremental usa `committed_at` como watermark.

**Cómo se usará:**
```python
from extractors.commits_extractor import CommitsExtractor
CommitsExtractor().run(mode="incremental")
```

**Por qué en este orden:** Depende conceptualmente de saber qué repos existen, aunque técnicamente llama a la API directamente. Se crea después de repos para mantener el orden lógico del pipeline.

---

### 9. `extractors/pull_requests_extractor.py` _(pendiente)_
**Qué hará:** Para cada repo, llama a `GET /repos/{owner}/{repo}/pulls?state=all` y carga en BigQuery. Aplana campos como `user.login`, `labels`, y `milestone.title`.

**Cómo se usará:**
```python
from extractors.pull_requests_extractor import PullRequestsExtractor
PullRequestsExtractor().run(mode="full")
```

**Por qué en este orden:** Es el extractor más complejo por la cantidad de campos anidados. Se deja para el final de los extractores para aplicar los patrones ya establecidos.

---

### 10. `main.py` _(pendiente)_
**Qué hará:** Punto de entrada del proceso. Recibe el argumento `--mode full` o `--mode incremental` y ejecuta los tres extractores en orden.

**Cómo se usará:**
```bash
python main.py --mode full
python main.py --mode incremental
```

**Por qué en este orden:** Se crea al final porque orquesta los extractores — no tiene sentido escribirlo antes de que existan.

---

### 11. `requirements.txt` _(pendiente)_
**Qué hará:** Lista todas las dependencias del proyecto con versiones fijas para garantizar reproducibilidad.

**Por qué en este orden:** Se genera cuando el código está completo para capturar exactamente lo que se usó.

---

### 12. `Dockerfile` _(pendiente)_
**Qué hará:** Define la imagen Docker que empaqueta el ETL para correr en Cloud Run. Copia el código, instala dependencias y define el comando de entrada.

**Por qué en este orden:** Se crea cuando el código está probado localmente y listo para desplegar.

---

### 13. `.env.example` _(pendiente)_
**Qué hará:** Documenta las variables de entorno necesarias para correr el proyecto localmente, sin valores reales (esos van en `.env` que no se commitea).

```bash
GCP_PROJECT_ID=tu-proyecto-gcp
LOG_LEVEL=INFO
```

**Por qué en este orden:** Se crea junto al Dockerfile como parte de la preparación para el despliegue.

---

### 14. `schedulers/full_refresh.yaml`
**Qué hace:** Define la configuración del Cloud Run Job y Cloud Scheduler para el full refresh: nombre, schedule cron, modo, región, memoria y timeout.

**Cómo se usa:** Se modifica via PR. Al mergear a `dev` o `main`, GitHub Actions lee este archivo y aplica los cambios automáticamente en Cloud Scheduler.

**Por qué en este orden:** Se crea al final porque depende de que el código y el contenedor estén listos.

---

### 15. `schedulers/incremental.yaml`
**Qué hace:** Igual que `full_refresh.yaml` pero para el job incremental — corre todos los días a las 6am UTC.

**Cómo se usa:** Mismo flujo via PR → GitHub Actions.

---

### 16. `.github/workflows/deploy_schedulers.yml`
**Qué hace:** Workflow de GitHub Actions que se dispara cuando se mergea un PR que modifica `schedulers/*.yaml`. Detecta la rama (`main` → producción en `reboost-moon-1`, `dev` → desarrollo en `vitrinasemp1`), se autentica con GCP y crea o actualiza el Cloud Run Job y el Cloud Scheduler correspondiente.

**Cómo se usa:**
1. Configurar secretos en GitHub: `GCP_SA_KEY_PROD` y `GCP_SA_KEY_DEV`
2. Modificar un YAML en `schedulers/` → abrir PR → mergear → el deploy es automático

**Por qué al final:** La infraestructura de CI/CD se define cuando el código está completo y listo para desplegar.

---

## Variables de entorno

| Variable | Descripción | Default |
|---|---|---|
| `GCP_PROJECT_ID` | ID del proyecto GCP | `vitrinasemp1` |
| `LOG_LEVEL` | Nivel de logging | `INFO` |

## Secretos en Secret Manager

| Nombre | Contenido |
|---|---|
| `api_info_github` | Token personal de GitHub |
| `bigquery_sa_key` | JSON de la Service Account de BigQuery |
