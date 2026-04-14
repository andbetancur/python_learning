import json
# para serializar el array de labels a string: json.dumps(["bug", "enhancement"]) → '["bug", "enhancement"]'

import pandas as pd
# para construir el DataFrame con los PRs transformados

import config
# constantes del proyecto: GCP_PROJECT_ID, BQ_DATASET, SECRET_GITHUB, SECRET_BQ_SA

from connectors.github_connector import GitHubConnector
# cliente HTTP para hablar con la API de GitHub

from connectors.bigquery_connector import BigQueryConnector
# cliente para leer y escribir en BigQuery

from models.schemas import PULL_REQUESTS_SCHEMA
# schema de la tabla pull_requests — define columnas y tipos esperados por BigQuery

from utils.logger import get_logger
# función que crea y configura un logger con formato estándar

logger = get_logger(__name__)
# __name__ vale "extractors.pull_requests_extractor"

TABLE_NAME = "pull_requests"
# nombre de la tabla destino en BigQuery

WATERMARK_COL = "updated_at"
# columna de fecha que usamos como watermark
# updated_at cambia cada vez que el PR se modifica, cierra o mergea

MERGE_KEYS = ["id"]
# id numérico del PR es la PK — identifica unívocamente cada PR en BigQuery


class PullRequestsExtractor:

    def __init__(self):
        # instancia los dos connectors al crear el extractor

        self.gh = GitHubConnector(config.GCP_PROJECT_ID, config.SECRET_GITHUB)
        # cliente GitHub autenticado con el token de Secret Manager

        self.bq = BigQueryConnector(config.GCP_PROJECT_ID, config.BQ_DATASET, config.SECRET_BQ_SA)
        # cliente BigQuery autenticado con la Service Account de Secret Manager

    def run(self, mode: str) -> None:
        # orquesta el flujo completo: repos → PRs → transform → filtrar → load
        # mode: "full" | "incremental"

        watermark = None
        # en modo full no filtramos nada, watermark queda en None

        if mode == "incremental":
            watermark = self.bq.get_watermark(TABLE_NAME, WATERMARK_COL)
            # SELECT MAX(updated_at) FROM pull_requests
            # retorna datetime UTC o None si la tabla está vacía → se comporta como full
            logger.info(f"Watermark: {watermark}")

        repos = self._fetch_repos()
        # obtiene la lista de full_names de repos del usuario
        # ejemplo: ["andbetancur/python_learning", "andbetancur/otro_repo"]

        all_rows = []
        # lista donde acumulamos todos los PRs de todos los repos

        for full_name in repos:
            # iteramos sobre cada repo y pedimos sus PRs

            raw = self._fetch_prs(full_name)
            # llama a GET /repos/{full_name}/pulls?state=all&sort=updated&direction=desc
            # trae todos los PRs del repo (abiertos, cerrados y mergeados)

            if not raw:
                logger.info(f"Sin PRs en {full_name}, saltando")
                continue
                # continue salta al siguiente repo si este no tiene ningún PR

            for pr in raw:
                all_rows.append(self._parse_pr(pr, full_name))
                # _parse_pr transforma un dict crudo en un dict limpio con los campos del schema

        if not all_rows:
            logger.info("Sin PRs, nada que cargar")
            return
            # si ningún repo tiene PRs, salimos sin tocar BigQuery

        df = self._transform(all_rows)
        # convierte la lista de dicts a DataFrame y parsea las cuatro columnas de fecha

        if mode == "incremental" and watermark:
            df = df[df["updated_at"] > watermark]
            # filtramos en Python porque la API de pulls no tiene parámetro "since"
            # nos quedamos solo con PRs actualizados después del watermark
            logger.info(f"{len(df)} PRs nuevos o actualizados desde {watermark}")

        if df.empty:
            logger.info("Sin datos nuevos, nada que cargar")
            return
            # puede pasar en incremental si nada cambió desde la última ejecución

        if mode == "full":
            self.bq.load_dataframe(df, TABLE_NAME, PULL_REQUESTS_SCHEMA)
            # WRITE_TRUNCATE: borra y recarga toda la tabla
        else:
            self.bq.upsert_dataframe(df, TABLE_NAME, PULL_REQUESTS_SCHEMA, MERGE_KEYS)
            # MERGE: actualiza PRs existentes e inserta los nuevos por id

    def _fetch_repos(self) -> list[str]:
        # obtiene la lista de repos y retorna solo los full_names
        # mismo método que en CommitsExtractor — extractor independiente, no lee de BQ

        logger.info("Obteniendo lista de repositorios...")

        repos = self.gh.get_paginated("/user/repos")
        # GET /user/repos → lista de repos del usuario autenticado

        return [r["full_name"] for r in repos]
        # list comprehension: extrae solo el full_name de cada repo

    def _fetch_prs(self, repo_full_name: str) -> list:
        # obtiene todos los PRs de UN repo específico
        # repo_full_name: "andbetancur/python_learning"

        logger.info(f"Obteniendo PRs de {repo_full_name}...")

        return self.gh.get_paginated(
            f"/repos/{repo_full_name}/pulls",
            params={
                "state":     "all",
                # "all" → trae PRs abiertos, cerrados y mergeados
                # sin este parámetro la API solo devuelve los abiertos
                # en incremental perderíamos los PRs que se cerraron desde el último run

                "sort":      "updated",
                # ordena por fecha de última actualización
                # junto con direction=desc, los más recientes llegan primero

                "direction": "desc",
                # descendente: el PR más recientemente actualizado es el primero
                # útil para el filtro de Python: los más viejos que el watermark quedan al final
            }
        )

    def _parse_pr(self, pr: dict, repo_full_name: str) -> dict:
        # transforma UN PR crudo de la API en un dict limpio con los campos del schema
        # pr: dict crudo | repo_full_name: string que agregamos nosotros

        gh_user = pr.get("user") or {}
        # user es el objeto GitHub del autor del PR
        # puede ser null en casos edge (cuentas eliminadas) → or {} evita AttributeError

        milestone = pr.get("milestone") or {}
        # milestone es un objeto con title, number, state, etc.
        # puede ser null si el PR no tiene milestone asignado → or {} para acceso seguro

        return {
            "id":                 pr["id"],
            # id numérico del PR — nunca null, es la PK

            "number":             pr["number"],
            # número del PR dentro del repo: #1, #2...
            # diferente al id: number es lo que ves en la URL de GitHub

            "repo_name":          repo_full_name,
            # lo agregamos nosotros — GitHub no lo incluye en la respuesta
            # necesario para saber a qué repo pertenece cada PR en BigQuery

            "title":              pr["title"],
            # título del PR

            "state":              pr["state"],
            # "open" o "closed" — mergeado también aparece como "closed"

            "user_login":         gh_user.get("login"),
            # login GitHub del autor: "andbetancur"
            # usamos gh_user (dict vacío si era null) → .get() retorna None si no existe

            "created_at":         pr["created_at"],
            # fecha de creación del PR — string ISO 8601, parseamos en _transform

            "updated_at":         pr["updated_at"],
            # fecha de última actualización — columna de watermark

            "closed_at":          pr.get("closed_at"),
            # fecha de cierre — null si el PR sigue abierto

            "merged_at":          pr.get("merged_at"),
            # fecha de merge — null si el PR fue cerrado sin mergear o sigue abierto

            "html_url":           pr["html_url"],
            # URL del PR en GitHub

            "base_branch":        pr["base"]["ref"],
            # rama destino del PR: "main"
            # pr["base"] es un objeto, accedemos a su campo "ref"

            "head_branch":        pr["head"]["ref"],
            # rama origen del PR: "feature/nueva-funcionalidad"
            # pr["head"] es un objeto, accedemos a su campo "ref"

            "draft":              pr.get("draft", False),
            # True si el PR es borrador — False por defecto si el campo no viene

            "locked":             pr["locked"],
            # True si el PR está bloqueado para comentarios

            "body":               pr.get("body"),
            # descripción del PR — puede ser null si no se escribió nada

            "author_association": pr.get("author_association"),
            # relación del autor con el repo: OWNER, CONTRIBUTOR, COLLABORATOR, etc.

            "labels":             json.dumps([l["name"] for l in pr.get("labels", [])]),
            # pr.get("labels", []) → lista de objetos label, o [] si no hay
            # [l["name"] for l in ...] → list comprehension que extrae solo el nombre de cada label
            # ["bug", "enhancement"] → json.dumps → '["bug", "enhancement"]'
            # guardamos como string porque BigQuery no acepta arrays en columnas STRING

            "milestone_title":    milestone.get("title"),
            # título del milestone: "v1.0", "Sprint 3", etc.
            # milestone ya es dict vacío si era null → .get() retorna None
        }

    def _transform(self, rows: list) -> pd.DataFrame:
        # convierte la lista de dicts limpios a un DataFrame
        # rows: lista de dicts que construimos en _parse_pr

        df = pd.DataFrame(rows)
        # cada dict de rows es una fila, cada clave es una columna

        for col in ["created_at", "updated_at", "closed_at", "merged_at"]:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
        # pd.to_datetime con utc=True → convierte strings ISO 8601 a datetime UTC
        # errors="coerce" → si el valor es null/None, lo convierte a NaT (Not a Time)
        #   en vez de lanzar un error
        # NaT en pandas equivale a NULL en BigQuery para columnas TIMESTAMP
        # closed_at y merged_at pueden ser null — por eso necesitamos errors="coerce"
        # created_at y updated_at nunca son null, pero los incluimos por consistencia

        logger.info(f"{len(df)} PRs transformados")
        return df
