import pandas as pd
# pandas para construir el DataFrame con los commits transformados

import config
# constantes del proyecto: GCP_PROJECT_ID, BQ_DATASET, SECRET_GITHUB, SECRET_BQ_SA

from connectors.github_connector import GitHubConnector
# cliente HTTP para hablar con la API de GitHub

from connectors.bigquery_connector import BigQueryConnector
# cliente para leer y escribir en BigQuery

from models.schemas import COMMITS_SCHEMA
# schema de la tabla commits — define columnas y tipos esperados por BigQuery

from utils.logger import get_logger
# función que crea y configura un logger con formato estándar

logger = get_logger(__name__)
# __name__ vale "extractors.commits_extractor"

TABLE_NAME = "commits"
# nombre de la tabla destino en BigQuery

WATERMARK_COL = "committed_at"
# columna de fecha que usamos como watermark
# corresponde a commit.author.date en el JSON de GitHub

MERGE_KEYS = ["sha"]
# el sha es la PK de un commit — hash único que identifica cada commit


class CommitsExtractor:

    def __init__(self):
        # instancia los dos connectors al crear el extractor

        self.gh = GitHubConnector(config.GCP_PROJECT_ID, config.SECRET_GITHUB)
        # cliente GitHub autenticado con el token de Secret Manager

        self.bq = BigQueryConnector(config.GCP_PROJECT_ID, config.BQ_DATASET, config.SECRET_BQ_SA)
        # cliente BigQuery autenticado con la Service Account de Secret Manager

    def run(self, mode: str) -> None:
        # orquesta el flujo completo: repos → commits → transform → load
        # mode: "full" | "incremental"

        watermark = None
        # en modo full no filtramos nada, watermark queda en None

        if mode == "incremental":
            watermark = self.bq.get_watermark(TABLE_NAME, WATERMARK_COL)
            # SELECT MAX(committed_at) FROM commits
            # si la tabla está vacía (primera ejecución) retorna None → se comporta como full
            logger.info(f"Watermark: {watermark}")

        repos = self._fetch_repos()
        # obtiene la lista de repos del usuario para saber a qué endpoints llamar
        # retorna una lista de full_names: ["andbetancur/python_learning", ...]

        all_rows = []
        # lista donde acumulamos todos los commits de todos los repos
        # al final la convertimos en un solo DataFrame

        for full_name in repos:
            # iteramos sobre cada repo y pedimos sus commits uno por uno

            raw = self._fetch_commits(full_name, since=watermark)
            # llama a GET /repos/{full_name}/commits con since= si hay watermark
            # si watermark es None, GitHub devuelve todos los commits del repo

            if not raw:
                logger.info(f"Sin commits en {full_name}, saltando")
                continue
                # continue salta al siguiente repo sin ejecutar el resto del bucle
                # evita agregar filas vacías al DataFrame

            for commit in raw:
                all_rows.append(self._parse_commit(commit, full_name))
                # _parse_commit transforma un dict crudo en un dict limpio con los campos del schema
                # lo agregamos a all_rows para construir el DataFrame al final

        if not all_rows:
            logger.info("Sin commits nuevos, nada que cargar")
            return
            # si no hay ningún commit nuevo en ningún repo, salimos sin tocar BigQuery

        df = self._transform(all_rows)
        # convierte la lista de dicts limpios a DataFrame y parsea las fechas

        if mode == "full":
            self.bq.load_dataframe(df, TABLE_NAME, COMMITS_SCHEMA)
            # WRITE_TRUNCATE: borra y recarga toda la tabla
        else:
            self.bq.upsert_dataframe(df, TABLE_NAME, COMMITS_SCHEMA, MERGE_KEYS)
            # MERGE: inserta commits nuevos, actualiza los existentes por sha

    def _fetch_repos(self) -> list[str]:
        # obtiene la lista de repos del usuario y retorna solo los full_names
        # -> list[str] → lista de strings, ejemplo: ["andbetancur/python_learning"]
        # llamamos a GitHub directamente para que este extractor sea independiente
        # no depende de que repos_extractor haya corrido antes

        logger.info("Obteniendo lista de repositorios...")

        repos = self.gh.get_paginated("/user/repos")
        # GET /user/repos → lista de repos del usuario autenticado

        return [r["full_name"] for r in repos]
        # list comprehension: extrae solo el full_name de cada repo
        # [r["full_name"] for r in repos] → ["andbetancur/python_learning", "andbetancur/otro_repo"]
        # solo necesitamos el full_name para construir el endpoint de commits

    def _fetch_commits(self, repo_full_name: str, since=None) -> list:
        # obtiene los commits de UN repo específico
        # repo_full_name: "andbetancur/python_learning"
        # since: datetime UTC o None — si viene, filtramos en la API directamente

        logger.info(f"Obteniendo commits de {repo_full_name}...")

        params = {}
        # dict vacío de parámetros — iremos agregando solo los que apliquen

        if since:
            params["since"] = since.isoformat()
            # since.isoformat() convierte el datetime a string ISO 8601
            # datetime(2024, 1, 15, 10, 30, tzinfo=UTC) → "2024-01-15T10:30:00+00:00"
            # GitHub acepta este formato y filtra commits más nuevos que esa fecha
            # esto es más eficiente que filtrar en Python: solo viajan los datos nuevos

        return self.gh.get_paginated(f"/repos/{repo_full_name}/commits", params=params)
        # GET /repos/andbetancur/python_learning/commits?since=2024-01-15T10:30:00+00:00
        # f-string construye el endpoint dinámicamente con el full_name del repo

    def _parse_commit(self, commit: dict, repo_full_name: str) -> dict:
        # transforma UN commit crudo de la API en un dict limpio con los campos del schema
        # separamos este método de _transform para mantener la lógica de transformación clara
        # commit: dict crudo de la API | repo_full_name: string que agregamos nosotros

        git_author = commit["commit"].get("author") or {}
        # commit["commit"] → objeto interno con los datos git del commit
        # .get("author") → puede ser null en commits malformados — retorna None
        # or {} → si es None, usamos dict vacío para que los .get() de abajo no fallen
        # sin este or {}, None.get("name") lanzaría AttributeError

        git_committer = commit["commit"].get("committer") or {}
        # mismo patrón para el committer git
        # committer puede diferir de author en rebases y merges

        gh_author = commit.get("author") or {}
        # author a nivel TOP del objeto — es el usuario GitHub vinculado al commit
        # distinto de git_author: este es la cuenta GitHub, no el nombre git
        # puede ser null si el email del commit no está vinculado a ninguna cuenta

        gh_committer = commit.get("committer") or {}
        # committer a nivel TOP — usuario GitHub del committer
        # mismo caso que gh_author: puede ser null

        verification = commit["commit"].get("verification") or {}
        # objeto con información de firma GPG/SSH del commit
        # or {} por si el campo no viene en la respuesta

        return {
            "sha":              commit["sha"],
            # hash único del commit — nunca null, es la PK

            "repo_name":        repo_full_name,
            # lo agregamos nosotros — GitHub no lo incluye en la respuesta de commits
            # necesario para saber a qué repo pertenece cada commit en BigQuery

            "message":          commit["commit"]["message"],
            # mensaje del commit — acceso de dos niveles: commit → commit → message

            "author_name":      git_author.get("name"),
            # nombre del autor git: "Andres Betancur"
            # usamos git_author (ya es dict vacío si era null) → .get() retorna None si no existe

            "author_email":     git_author.get("email"),
            # email del autor git

            "committed_at":     git_author.get("date"),
            # fecha del commit — string ISO 8601, lo convertimos a datetime en _transform
            # usamos la fecha del author, no del committer (es la fecha original del trabajo)

            "committer_name":   git_committer.get("name"),
            # nombre de quien ejecutó el commit físicamente

            "committer_login":  gh_committer.get("login"),
            # login GitHub del committer — None si no tiene cuenta vinculada

            "author_login":     gh_author.get("login"),
            # login GitHub del autor — None si el email no está vinculado a una cuenta

            "comment_count":    commit["commit"].get("comment_count"),
            # número de comentarios en el commit

            "verified":         verification.get("verified"),
            # True si el commit tiene firma GPG/SSH válida — None si no hay objeto verification

            "parent_count":     len(commit.get("parents", [])),
            # calculamos el número de padres desde el array parents
            # commit.get("parents", []) → si no viene el campo, usamos lista vacía
            # len([]) → 0 | len([{sha: "abc"}]) → 1 | len([...2 items]) → 2 (merge commit)

            "html_url":         commit["html_url"],
            # URL del commit en GitHub
        }

    def _transform(self, rows: list) -> pd.DataFrame:
        # convierte la lista de dicts limpios (ya parseados) a un DataFrame
        # rows: lista de dicts que construimos en _parse_commit
        # -> pd.DataFrame listo para cargar en BigQuery

        df = pd.DataFrame(rows)
        # cada dict de rows es una fila, cada clave es una columna

        df["committed_at"] = pd.to_datetime(df["committed_at"], utc=True)
        # convierte el string "2024-01-15T10:30:00Z" a datetime UTC
        # utc=True → timezone-aware, necesario para BigQuery y para comparar con el watermark

        logger.info(f"{len(df)} commits transformados")
        return df
