import json
# librería estándar de Python — no necesita instalarse
# la usamos para convertir el array de topics a string: json.dumps(["etl", "python"]) → '["etl", "python"]'
# BigQuery no acepta arrays nativos en columnas STRING, por eso serializamos

import pandas as pd
# pandas — librería para trabajar con datos en forma de tabla (DataFrame)
# un DataFrame es como una hoja de Excel en memoria: filas y columnas
# BigQuery acepta DataFrames directamente para cargar datos

import config
# nuestro config.py — contiene GCP_PROJECT_ID, BQ_DATASET, SECRET_GITHUB, SECRET_BQ_SA
# lo importamos para no repetir strings en el código

from connectors.github_connector import GitHubConnector
# importamos la clase que maneja toda la comunicación HTTP con GitHub
# se encarga de autenticación, paginación y rate limit

from connectors.bigquery_connector import BigQueryConnector
# importamos la clase que maneja toda la interacción con BigQuery
# se encarga de cargas full, upserts incrementales y watermarks

from models.schemas import REPOS_SCHEMA
# importamos el schema de la tabla repositories
# define las columnas y tipos que BigQuery espera recibir

from utils.logger import get_logger
# función que crea y configura un logger con formato estándar

logger = get_logger(__name__)
# crea el logger para este módulo
# __name__ vale "extractors.repos_extractor" — aparece en cada línea del log

TABLE_NAME = "repositories"
# nombre de la tabla destino en BigQuery
# se usa en load_dataframe(), upsert_dataframe() y get_watermark()

WATERMARK_COL = "updated_at"
# columna de fecha nativa de GitHub que usamos para saber qué cambió
# en incremental: traemos solo repos con updated_at > último valor en BQ

MERGE_KEYS = ["id"]
# columna(s) que identifican unívocamente un repo en BigQuery
# el MERGE usa esta(s) clave(s) para decidir si actualizar o insertar


class ReposExtractor:
    # clase que encapsula toda la lógica del extractor de repositorios
    # sigue el mismo patrón que usarán CommitsExtractor y PullRequestsExtractor

    def __init__(self):
        # constructor: se ejecuta cuando haces ReposExtractor()
        # instancia los dos connectors que este extractor necesita

        self.gh = GitHubConnector(config.GCP_PROJECT_ID, config.SECRET_GITHUB)
        # crea el cliente de GitHub autenticado con el token guardado en Secret Manager
        # self.gh queda disponible para todos los métodos de la clase

        self.bq = BigQueryConnector(config.GCP_PROJECT_ID, config.BQ_DATASET, config.SECRET_BQ_SA)
        # crea el cliente de BigQuery autenticado con la Service Account de Secret Manager
        # self.bq queda disponible para todos los métodos de la clase

    def run(self, mode: str) -> None:
        # método principal — orquesta el flujo completo del extractor
        # mode: "full" → recarga toda la tabla | "incremental" → solo cambios nuevos
        # -> None indica que no retorna ningún valor, solo ejecuta efectos (carga en BQ)

        watermark = None
        # inicializamos watermark en None
        # si mode es "full" se queda en None y no filtramos nada

        if mode == "incremental":
            watermark = self.bq.get_watermark(TABLE_NAME, WATERMARK_COL)
            # consulta SELECT MAX(updated_at) FROM repositories en BigQuery
            # retorna un datetime UTC si la tabla tiene datos, o None si está vacía
            # None en primera ejecución = comportamiento igual a "full"
            logger.info(f"Watermark: {watermark}")

        raw = self._fetch()
        # llama al método interno que habla con la API de GitHub
        # retorna una lista de dicts, uno por repositorio

        df = self._transform(raw)
        # convierte la lista de dicts a un DataFrame de pandas
        # aplana campos anidados y convierte fechas a datetime

        if mode == "incremental" and watermark:
            df = df[df["updated_at"] > watermark]
            # filtra el DataFrame para quedarse solo con repos más nuevos que el watermark
            # df["updated_at"] > watermark → genera una serie de True/False por fila
            # df[...] → aplica la máscara y devuelve solo las filas donde es True
            # nota: la API de repos no tiene parámetro "since", por eso filtramos en Python
            logger.info(f"{len(df)} repos nuevos o actualizados desde {watermark}")

        if df.empty:
            logger.info("Sin datos nuevos, nada que cargar")
            return
            # si el DataFrame quedó vacío (nada cambió desde el último incremental)
            # salimos sin hacer ninguna llamada a BigQuery

        if mode == "full":
            self.bq.load_dataframe(df, TABLE_NAME, REPOS_SCHEMA)
            # WRITE_TRUNCATE: borra toda la tabla y carga los datos nuevos
        else:
            self.bq.upsert_dataframe(df, TABLE_NAME, REPOS_SCHEMA, MERGE_KEYS)
            # MERGE: actualiza repos existentes e inserta los nuevos

    def _fetch(self) -> list:
        # método interno (el _ indica que no se llama desde afuera de la clase)
        # responsabilidad única: hablar con la API y retornar datos crudos
        # -> list indica que retorna una lista de dicts

        logger.info("Obteniendo repositorios desde GitHub...")

        return self.gh.get_paginated("/user/repos", params={"sort": "updated", "direction": "desc"})
        # GET /user/repos → repos del usuario autenticado por el token
        # sort=updated    → ordena por fecha de última actualización
        # direction=desc  → los más recientemente actualizados primero
        # esto ayuda en incremental: si el primero ya es más viejo que el watermark,
        # el filtro de Python descartará todo rápidamente sin procesar filas innecesarias

    def _transform(self, raw: list) -> pd.DataFrame:
        # método interno que convierte los dicts crudos de la API en un DataFrame limpio
        # raw: list → la lista de dicts que devolvió _fetch()
        # -> pd.DataFrame → retorna un DataFrame listo para cargar en BigQuery

        rows = []
        # lista vacía donde iremos acumulando un dict por cada repo transformado

        for repo in raw:
            # iteramos sobre cada repo de la respuesta de la API
            # repo es un dict con todos los campos que devuelve GitHub

            rows.append({
            # construimos un dict con exactamente los campos del schema
            # el orden no importa — BigQuery los mapea por nombre, no por posición

                "id":                repo["id"],
                # id numérico del repo — usamos [] porque es campo requerido, nunca null

                "name":              repo["name"],
                # nombre corto: "python_learning"

                "full_name":         repo["full_name"],
                # nombre completo: "andbetancur/python_learning"

                "description":       repo.get("description"),
                # .get() en vez de [] porque description puede ser null
                # repo.get("description") retorna None si el campo no existe o es null
                # None en pandas se convierte automáticamente a NULL en BigQuery

                "html_url":          repo["html_url"],
                # URL del repo en GitHub

                "language":          repo.get("language"),
                # puede ser null si GitHub no detectó lenguaje principal

                "stargazers_count":  repo["stargazers_count"],
                "forks_count":       repo["forks_count"],
                "open_issues_count": repo["open_issues_count"],

                "created_at":        repo["created_at"],
                # string ISO 8601: "2024-01-15T10:30:00Z"
                # lo convertimos a datetime más abajo con pd.to_datetime()

                "updated_at":        repo["updated_at"],
                # columna de watermark — la más importante para el incremental

                "pushed_at":         repo.get("pushed_at"),
                # puede ser null en repos sin ningún push

                "visibility":        repo.get("visibility"),
                # "public" o "private"

                "default_branch":    repo["default_branch"],
                # "main" o "master"

                "owner_login":       repo["owner"]["login"],
                # campo anidado: repo["owner"] es un dict, accedemos a su campo "login"
                # repo["owner"]["login"] → "andbetancur"
                # usamos [] porque owner y owner.login siempre existen en la respuesta

                "fork":              repo["fork"],
                # True si este repo es fork de otro

                "archived":          repo["archived"],
                # True si el repo está archivado (read-only en GitHub)

                "private":           repo["private"],
                # True si el repo es privado

                "watchers_count":    repo["watchers_count"],
                "size":              repo["size"],
                # tamaño en kilobytes

                "topics":            json.dumps(repo.get("topics", [])),
                # topics es un array: ["python", "etl", "bigquery"]
                # json.dumps() lo convierte a string: '["python", "etl", "bigquery"]'
                # repo.get("topics", []) → si el campo no viene, usamos lista vacía
                # json.dumps([]) → '[]' (string vacío en formato JSON, no null)
            })

        df = pd.DataFrame(rows)
        # pd.DataFrame(rows) convierte la lista de dicts en un DataFrame
        # cada dict es una fila, cada clave del dict es una columna

        for col in ["created_at", "updated_at", "pushed_at"]:
            df[col] = pd.to_datetime(df[col], utc=True)
        # pd.to_datetime convierte el string "2024-01-15T10:30:00Z" a un objeto datetime de Python
        # utc=True → fuerza que todos los datetimes sean timezone-aware en UTC
        # esto es necesario por dos razones:
        #   1. BigQuery requiere objetos datetime para columnas TIMESTAMP
        #   2. el watermark que viene de BigQuery también es datetime UTC
        #      → si no forzamos UTC aquí, la comparación df["updated_at"] > watermark falla
        #         porque no se puede comparar datetime con timezone vs sin timezone

        logger.info(f"{len(df)} repositorios transformados")
        return df
