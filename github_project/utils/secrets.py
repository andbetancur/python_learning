from google.cloud import secretmanager
# cliente oficial de Google para interactuar con Secret Manager
# se instala con: pip install google-cloud-secret-manager

from functools import lru_cache
# lru_cache es un decorador de la librería estándar que cachea los resultados de una función
# "Least Recently Used cache" — guarda los resultados en memoria para no repetir llamadas costosas


@lru_cache(maxsize=None)
# decorador que envuelve get_secret con un caché en memoria
# maxsize=None → el caché no tiene límite de entradas
# efecto: si llamas get_secret("proj", "github-token") dos veces,
#         la segunda vez devuelve el valor guardado sin ir a GCP
def get_secret(project_id: str, secret_name: str, version: str = "latest") -> str:
    # project_id  → el ID de tu proyecto en GCP (ej: "my-gcp-project")
    # secret_name → el nombre del secreto en Secret Manager (ej: "api_info_github")
    # version     → qué versión del secreto quieres, por defecto la más reciente
    # -> str      → retorna el valor del secreto como string

    client = secretmanager.SecretManagerServiceClient()
    # crea el cliente de Secret Manager
    # usa Application Default Credentials (ADC) automáticamente:
    # en local → usa tu cuenta de gcloud (gcloud auth application-default login)
    # en GCP   → usa la Service Account asignada al recurso (VM, Cloud Run, etc.)
    # no necesitas pasar credenciales explícitamente

    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    # construye el "resource name" que GCP usa para identificar un secreto
    # formato obligatorio de la API: projects/{project}/secrets/{secret}/versions/{version}
    # ejemplo: "projects/my-project/secrets/api_info_github/versions/latest"

    response = client.access_secret_version(request={"name": secret_path})
    # llama a la API de Secret Manager para obtener el valor del secreto
    # access_secret_version → el método de la API que lee el contenido de una versión
    # request={"name": secret_path} → le dice a la API qué secreto queremos

    return response.payload.data.decode("UTF-8")
    # response.payload.data → el valor del secreto en bytes (formato binario)
    # .decode("UTF-8")      → convierte los bytes a string legible
    # retorna el valor limpio: el token, el JSON de la SA, etc.
