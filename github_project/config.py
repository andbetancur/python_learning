import os
# librería estándar para leer variables de entorno
# la usamos para obtener el GCP_PROJECT_ID sin hardcodearlo

GCP_PROJECT_ID: str = os.environ.get("GCP_PROJECT_ID", "vitrinasemp1")
# lee la variable de entorno GCP_PROJECT_ID del sistema
# si no existe, usa "your-gcp-project-id" como placeholder
# : str → type hint, indica que esta variable es un string
# en local puedes hacer: export GCP_PROJECT_ID=mi-proyecto
# en producción (Cloud Run, VM) se configura como variable de entorno del servicio

BQ_DATASET: str = "github_project"
# nombre del dataset en BigQuery donde se crearán las tablas
# repositories, commits y pull_requests vivirán dentro de este dataset

GITHUB_USERNAME: str = "andbetancur"
# tu usuario de GitHub
# los extractores lo usan para construir las URLs de la API
# ejemplo: /users/AndresdBetancur/repos

SECRET_GITHUB: str = "api_info_github"
# nombre exacto del secreto en Google Secret Manager que contiene tu token de GitHub
# debe coincidir con el nombre que le diste al crearlo en GCP

SECRET_BQ_SA: str = "bigquery_sa_key"
# nombre exacto del secreto en Google Secret Manager que contiene
# el JSON de la Service Account de BigQuery
