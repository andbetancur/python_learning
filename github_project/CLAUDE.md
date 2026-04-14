# github_project — ETL GitHub → BigQuery

## Objetivo del proyecto
Framework de extracción de datos para aprender a usar bien `requests`, `pandas`
y la interacción con APIs REST. Extrae datos de la API de GitHub y los carga en
BigQuery, usando Secret Manager para manejar credenciales de forma segura.
Soporta dos modos de carga: **full refresh** e **incremental** (watermark-based).

## Objetivo final
El código está completo. Pendiente:
1. Crear la Service Account de BigQuery y guardar el JSON en Secret Manager como `bigquery_sa_key`
2. Construir y subir la imagen Docker a Artifact Registry
3. Configurar secretos `GCP_SA_KEY_PROD` y `GCP_SA_KEY_DEV` en GitHub Actions
4. Mergear a `dev` → GitHub Actions despliega Cloud Run Job + Cloud Scheduler en `vitrinasemp1`
5. Mergear a `main` → despliega en `reboost-moon-1` (producción)

El deploy de schedulers es automático via GitHub Actions al mergear PRs que toquen `schedulers/*.yaml`.

---

## Arquitectura

```
python_learning/
├── .github/
│   └── workflows/
│       └── deploy_schedulers.yml    # CI/CD: despliega schedulers al mergear a dev o main
└── github_project/
    ├── config.py                        # constantes globales (project_id, dataset, usuario, secretos)
    ├── utils/
    │   ├── logger.py                    # logger centralizado con formato y nivel configurable via LOG_LEVEL
    │   └── secrets.py                   # get_secret() con lru_cache → Secret Manager
    ├── connectors/
    │   ├── github_connector.py          # HTTP client: paginación, rate limit 403, backoff exponencial
    │   └── bigquery_connector.py        # BQ client: load (WRITE_TRUNCATE), upsert (MERGE), get_watermark
    ├── models/
    │   └── schemas.py                   # REPOS_SCHEMA, COMMITS_SCHEMA, PULL_REQUESTS_SCHEMA
    ├── extractors/
    │   ├── repos_extractor.py           # extrae /user/repos → tabla repositories
    │   ├── commits_extractor.py         # extrae /repos/{owner}/{repo}/commits → tabla commits
    │   └── pull_requests_extractor.py   # extrae /repos/{owner}/{repo}/pulls → tabla pull_requests
    ├── schedulers/
    │   ├── full_refresh.yaml            # config del job FR (domingos 2am UTC)
    │   └── incremental.yaml             # config del job incremental (diario 6am UTC)
    ├── main.py                          # punto de entrada: --mode full | incremental [--extractor nombre]
    ├── requirements.txt
    ├── Dockerfile
    ├── .env.example
    ├── README.md
    └── CLAUDE.md
```

## Tablas en BigQuery (dataset: github_project)

| Tabla | PK | Watermark |
|---|---|---|
| repositories | id | updated_at |
| commits | sha | committed_at |
| pull_requests | id | updated_at |

## Flujo de un extractor

1. Instancia `BigQueryConnector` y `GitHubConnector`
2. Si modo **incremental**: llama `bq.get_watermark(table, col)` → filtra la API con `since=`
3. Llama `github.get_paginated(endpoint)` → lista de dicts crudos
4. Transforma a `pd.DataFrame` seleccionando y renombrando campos
5. Si modo **full**: llama `bq.load_dataframe(df, table, schema)` (WRITE_TRUNCATE)
6. Si modo **incremental**: llama `bq.upsert_dataframe(df, table, schema, merge_keys)`

## Decisiones de diseño clave

- `process_date` la agrega automáticamente el `BigQueryConnector`, los extractores no la tocan
- El watermark usa la fecha nativa de GitHub (`updated_at`, `committed_at`), no `process_date`
- Las credenciales nunca tocan disco: Secret Manager → memoria → cliente
- `lru_cache` en `get_secret` evita llamadas repetidas a GCP en la misma ejecución
- `GitHubConnector` maneja rate limit (403 + `X-RateLimit-Reset`) y errores 5xx con backoff

## Estado actual

- [x] config.py
- [x] utils/logger.py
- [x] utils/secrets.py
- [x] connectors/github_connector.py
- [x] connectors/bigquery_connector.py
- [x] models/schemas.py
- [x] extractors/repos_extractor.py
- [x] extractors/commits_extractor.py
- [x] extractors/pull_requests_extractor.py
- [x] main.py
- [x] requirements.txt
- [x] Dockerfile
- [x] .env.example
- [x] schedulers/full_refresh.yaml
- [x] schedulers/incremental.yaml
- [x] .github/workflows/deploy_schedulers.yml
- [ ] Crear SA de BigQuery + guardar JSON en Secret Manager
- [ ] Construir y subir imagen Docker a Artifact Registry
- [ ] Configurar GCP_SA_KEY_PROD y GCP_SA_KEY_DEV en GitHub Secrets
- [ ] Primer deploy a dev y validar end-to-end

## Variables de entorno relevantes

| Variable | Uso | Default |
|---|---|---|
| `GCP_PROJECT_ID` | ID del proyecto GCP | `vitrinasemp1` |
| `LOG_LEVEL` | Nivel de logging | `INFO` |

## Secretos en Secret Manager

| Nombre | Contenido |
|---|---|
| `api_info_github` | Token personal de GitHub |
| `bigquery_sa_key` | JSON de la Service Account de BigQuery |
