import json
# librería estándar para convertir el string JSON de la SA a diccionario de Python
# json.loads → string a dict | json.dumps → dict a string

import time
# para generar timestamps únicos en el nombre de la tabla temporal del MERGE

from datetime import datetime, timezone
# datetime → para generar el valor de process_date en el momento de la carga
# timezone → para que la fecha sea UTC, estándar en data engineering

import pandas as pd
# pandas — trabajamos con DataFrames: tablas en memoria con filas y columnas
# BigQuery acepta DataFrames directamente para cargar datos

from google.cloud import bigquery
# cliente oficial de Google para interactuar con BigQuery
# maneja jobs de carga, queries SQL, schemas, etc.

from google.oauth2 import service_account
# módulo de Google para crear credenciales desde un JSON de Service Account
# lo usamos porque el secreto en Secret Manager es el JSON de la SA, no un token directo

from utils.secrets import get_secret
# para obtener el JSON de la Service Account desde Secret Manager

from utils.logger import get_logger
# nuestro logger centralizado

logger = get_logger(__name__)
# __name__ vale "connectors.bigquery_connector" automáticamente


class BigQueryConnector:

    def __init__(self, project_id: str, dataset: str, secret_name: str):
        # project_id  → ID del proyecto GCP: "vitrinasemp1" o "reboost-moon-1"
        # dataset     → nombre del dataset en BQ: "github_project"
        # secret_name → nombre del secreto en Secret Manager: "bigquery_sa_key"

        self.project_id = project_id
        # guardamos project_id como atributo — se usa en table_ref() y en el cliente

        self.dataset = dataset
        # nombre del dataset — se usa en table_ref()

        sa_key_json = get_secret(project_id, secret_name)
        # obtiene el JSON de la Service Account desde Secret Manager como string
        # ejemplo: '{"type": "service_account", "project_id": "...", "private_key": "..."}'

        sa_info = json.loads(sa_key_json)
        # convierte el string JSON a diccionario de Python
        # json.loads = "load from string" → string a dict

        credentials = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
            # scopes → qué permisos pedimos al autenticarnos
            # cloud-platform cubre BigQuery y otros servicios de GCP
            # nunca escribimos el JSON a disco — vivió solo en memoria
        )
        # from_service_account_info crea credenciales directamente desde el dict
        # equivalente a from_service_account_file() pero sin archivo en disco

        self.client = bigquery.Client(project=project_id, credentials=credentials)
        # crea el cliente de BigQuery autenticado con la SA
        # todos los métodos del conector usan self.client para operar en BQ

    def table_ref(self, table_name: str) -> str:
        return f"{self.project_id}.{self.dataset}.{table_name}"
        # BigQuery identifica tablas con el formato: proyecto.dataset.tabla
        # ejemplo: "vitrinasemp1.github_project.repositories"
        # método helper para no repetir esta concatenación en cada método

    def _add_process_date(self, df: pd.DataFrame) -> pd.DataFrame:
        # método interno que agrega la columna process_date al DataFrame
        # se llama automáticamente antes de cada carga — los extractores no se preocupan por esto
        df = df.copy()
        # .copy() evita modificar el DataFrame original que vino del extractor
        # en pandas, las asignaciones sin copy pueden modificar el objeto original por referencia
        df["process_date"] = datetime.now(timezone.utc)
        # datetime.now(timezone.utc) → fecha y hora actual en UTC
        # UTC es el estándar en data engineering — evita problemas de zonas horarias
        # esta columna nos dice exactamente cuándo corrió el ETL para cada fila
        return df

    def load_dataframe(self, df: pd.DataFrame, table_name: str, schema: list) -> None:
        # full refresh: borra todo el contenido de la tabla y carga los datos nuevos
        # df         → DataFrame con los datos del extractor
        # table_name → nombre de la tabla destino en BigQuery
        # schema     → lista de SchemaField que define columnas y tipos (viene de models/schemas.py)
        # -> None    → no retorna nada, solo ejecuta la carga

        df = self._add_process_date(df)
        # agrega process_date antes de cargar — auditoría de cuándo se procesó cada fila

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            # le dice a BigQuery exactamente qué columnas esperar y de qué tipo
            # sin schema BigQuery intenta inferirlo — propenso a errores con nulls y fechas
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            # WRITE_TRUNCATE → borra todo el contenido de la tabla y carga los datos nuevos
            # comportamiento de "full refresh": tabla limpia en cada ejecución
            # WRITE_APPEND → agrega sin borrar (riesgo de duplicados)
            # WRITE_EMPTY  → falla si la tabla ya tiene datos
        )

        table = self.table_ref(table_name)
        # "vitrinasemp1.github_project.repositories"

        job = self.client.load_table_from_dataframe(df, table, job_config=job_config)
        # convierte el DataFrame a Parquet internamente y lo envía a BQ como job asíncrono
        # "job" representa la operación en curso — aún no terminó

        job.result()
        # .result() bloquea la ejecución hasta que el job termine
        # si el job falla, lanza una excepción con el detalle del error de BigQuery

        logger.info(f"Cargadas {len(df)} filas en {table} (WRITE_TRUNCATE)")

    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, schema: list, merge_keys: list[str]) -> None:
        # incremental: inserta filas nuevas y actualiza las existentes sin duplicar
        # merge_keys → columnas que identifican unívocamente una fila
        # repos: ["id"] | commits: ["sha"] | pull_requests: ["id"]

        df = self._add_process_date(df)
        # agrega process_date — en incremental esta fecha refleja cuándo se actualizó la fila

        temp_table = f"{table_name}_temp_{int(time.time())}"
        # nombre único para la tabla temporal usando timestamp Unix
        # ejemplo: "repositories_temp_1712345678"
        # el timestamp garantiza que dos procesos paralelos no se pisen

        temp_ref = self.table_ref(temp_table)
        target_ref = self.table_ref(table_name)

        # --- paso 1: carga los datos nuevos en una tabla temporal ---
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        job = self.client.load_table_from_dataframe(df, temp_ref, job_config=job_config)
        job.result()
        # los datos nuevos ya están en la tabla temporal, listos para el MERGE

        # --- paso 2: construye el SQL del MERGE dinámicamente ---
        merge_condition = " AND ".join(f"T.{k} = S.{k}" for k in merge_keys)
        # condición ON del MERGE — une las merge_keys con AND
        # merge_keys=["id"]         → "T.id = S.id"
        # merge_keys=["repo", "sha"] → "T.repo = S.repo AND T.sha = S.sha"

        columns = [field.name for field in schema]
        # extrae los nombres de columnas del schema
        # ejemplo: ["id", "name", "full_name", "updated_at", "process_date", ...]

        update_set = ", ".join(f"T.{c} = S.{c}" for c in columns if c not in merge_keys)
        # SET del UPDATE — actualiza todas las columnas excepto las merge_keys
        # no actualizamos la PK porque fue la que usamos para hacer match
        # ejemplo: "T.name = S.name, T.updated_at = S.updated_at, T.process_date = S.process_date"

        insert_cols = ", ".join(columns)
        # columnas para el INSERT: "id, name, full_name, updated_at, process_date, ..."

        insert_vals = ", ".join(f"S.{c}" for c in columns)
        # valores del INSERT tomados de S (Source = tabla temporal)
        # "S.id, S.name, S.full_name, S.updated_at, S.process_date, ..."

        merge_sql = f"""
        MERGE `{target_ref}` T
        USING `{temp_ref}` S
        ON {merge_condition}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        # MERGE es SQL estándar de BigQuery (equivalente a UPSERT en otras DBs)
        # T = Target → tabla destino que queremos actualizar
        # S = Source → tabla temporal con los datos nuevos
        # WHEN MATCHED     → la fila ya existe en T → actualizamos sus valores
        # WHEN NOT MATCHED → la fila es nueva → la insertamos

        self.client.query(merge_sql).result()
        # ejecuta el MERGE y espera que termine

        self.client.delete_table(temp_ref)
        # elimina la tabla temporal — ya no la necesitamos y ocupa espacio en BQ

        logger.info(f"Upsert de {len(df)} filas en {target_ref}")

    def get_watermark(self, table_name: str, watermark_column: str) -> str | None:
        # obtiene el valor máximo de la columna de fecha nativa de GitHub
        # watermark_column → "updated_at" (repos, PRs) o "committed_at" (commits)
        # NO usamos process_date como watermark — esa es nuestra fecha de proceso,
        # no la fecha en que GitHub modificó el objeto (ver explicación en bigquery_connector)
        # retorna el valor máximo como datetime, o None si la tabla está vacía o no existe

        query = f"SELECT MAX({watermark_column}) as wm FROM `{self.table_ref(table_name)}`"
        # MAX() → el valor más reciente de la columna de fecha
        # si la tabla tiene datos hasta 2026-04-10, retorna datetime(2026, 4, 10, ...)
        # el extractor le pasa ese valor a la API de GitHub como parámetro "since"

        try:
            result = self.client.query(query).result()
            for row in result:
                return row.wm
                # row.wm → accede al campo "wm" del resultado (el alias del SELECT)
                # BigQuery retorna un objeto datetime de Python para columnas TIMESTAMP
        except Exception:
            return None
            # si la tabla no existe (primera ejecución) la query falla
            # retornamos None para que el extractor haga una carga completa
