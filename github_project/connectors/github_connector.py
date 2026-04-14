import time
# librería estándar para manejar tiempos y pausas
# la usamos para calcular cuánto esperar cuando GitHub nos dice "rate limit"

import requests
# librería para hacer peticiones HTTP — la más usada en Python
# NO es estándar, se instala: pip install requests

import config
# nuestro config.py — lo importamos para no repetir strings

from utils.secrets import get_secret
# para obtener el token de GitHub desde Secret Manager

from utils.logger import get_logger
# nuestro logger centralizado

logger = get_logger(__name__)
# crea el logger para este módulo
# __name__ vale "connectors.github_connector" automáticamente


class GitHubConnector:
    # clase que encapsula toda la lógica de comunicación con GitHub
    # todos los extractores la usan — ellos nunca hacen requests directamente

    BASE_URL = "https://api.github.com"
    # URL base de la API de GitHub — se combina con cada endpoint
    # ejemplo: BASE_URL + "/user/repos" → "https://api.github.com/user/repos"

    MAX_RETRIES = 3
    # número máximo de reintentos si una petición falla por error del servidor

    BACKOFF_FACTOR = 2
    # factor para el backoff exponencial: espera 2^intento segundos entre reintentos
    # intento 1 → espera 2s, intento 2 → espera 4s, intento 3 → espera 8s

    def __init__(self, project_id: str, secret_name: str):
        # constructor: se ejecuta cuando haces GitHubConnector(...)
        # project_id  → ID del proyecto GCP para ir a buscar el secreto
        # secret_name → nombre del secreto en Secret Manager

        token = get_secret(project_id, secret_name)
        # obtiene el token de GitHub desde Secret Manager
        # el token nunca se guarda como atributo de la clase para no exponerlo en memoria más de lo necesario

        self._session = requests.Session()
        # Session() reutiliza la conexión TCP entre peticiones — más eficiente que requests.get() individual
        # el _ al inicio indica que es un atributo "privado" — convención en Python

        self._session.headers.update({
            "Authorization": f"token {token}",
            # le dice a GitHub quién eres — todas las peticiones llevan este header automáticamente
            "Accept": "application/vnd.github.v3+json",
            # le pide a GitHub que responda en formato JSON con la versión 3 de la API
            "X-GitHub-Api-Version": "2022-11-28",
            # versión específica de la API — garantiza que el comportamiento no cambia si GitHub actualiza
        })

    def _request(self, endpoint: str, params: dict = None) -> dict | list:
        # método interno (el _ indica que no se llama desde afuera)
        # endpoint → la ruta de la API, ej: "/user/repos"
        # params   → query parameters opcionales, ej: {"per_page": 100}
        # retorna dict o list según lo que devuelva la API

        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        # construye la URL completa
        # lstrip('/') elimina el / inicial del endpoint si existe, para evitar doble //
        # "/user/repos" y "user/repos" producen el mismo resultado

        for attempt in range(self.MAX_RETRIES):
            # intenta hasta MAX_RETRIES veces antes de rendirse

            response = self._session.get(url, params=params)
            # hace la petición GET con los headers ya configurados en la sesión

            if response.status_code == 200:
                return response.json()
                # éxito — convierte el JSON de la respuesta a dict/list de Python y retorna

            if response.status_code == 403:
                # 403 = Forbidden — casi siempre significa rate limit de GitHub
                reset_time = int(response.headers.get("X-RateLimit-Reset", time.time() + 60))
                # X-RateLimit-Reset → header que GitHub envía con el timestamp Unix de cuando se resetea el límite
                # si no viene el header, esperamos 60 segundos por defecto
                sleep_time = max(reset_time - time.time(), 1)
                # calcula cuántos segundos faltan para el reset
                # max(..., 1) garantiza que esperamos al menos 1 segundo
                logger.warning(f"Rate limit alcanzado. Esperando {sleep_time:.0f}s")
                time.sleep(sleep_time)
                # pausa la ejecución hasta que GitHub permita nuevas peticiones

            elif response.status_code in (500, 502, 503, 504):
                # errores del servidor de GitHub — transitorios, vale la pena reintentar
                wait = self.BACKOFF_FACTOR ** attempt
                # backoff exponencial: 2^0=1s, 2^1=2s, 2^2=4s
                logger.warning(f"Error {response.status_code} del servidor. Reintento {attempt + 1} en {wait}s")
                time.sleep(wait)

            else:
                response.raise_for_status()
                # para cualquier otro error (404, 401, etc.) lanza una excepción inmediatamente
                # no tiene sentido reintentar un 404 — el recurso no existe

        raise RuntimeError(f"Falló después de {self.MAX_RETRIES} intentos: {url}")
        # si agotamos todos los reintentos, lanzamos un error claro

    def get(self, endpoint: str, params: dict = None) -> dict | list:
        # método simple para obtener un único recurso
        # ejemplo: connector.get("/users/AndresdBetancur")
        return self._request(endpoint, params)

    def get_paginated(self, endpoint: str, params: dict = None) -> list:
        # método para endpoints que devuelven listas grandes divididas en páginas
        # GitHub devuelve máximo 100 items por página — este método las junta todas

        params = params or {}
        # si no se pasaron params, inicializa un dict vacío

        params["per_page"] = 100
        # le pide a GitHub el máximo de items por página para minimizar el número de peticiones

        page = 1
        results = []

        while True:
            params["page"] = page
            # le dice a GitHub qué página queremos

            data = self._request(endpoint, params)

            if not data:
                break
                # si la página vino vacía, ya no hay más datos

            results.extend(data if isinstance(data, list) else [data])
            # extend agrega los items de la página al acumulador
            # isinstance(data, list) maneja el caso donde la API devuelve un solo objeto en vez de lista

            if len(data) < params["per_page"]:
                break
                # si la página vino con menos items que el máximo, era la última página

            page += 1

        return results
        # retorna todos los items de todas las páginas en una sola lista
