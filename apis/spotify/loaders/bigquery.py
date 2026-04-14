"""
bigquery.py — Loader para cargar DataFrames a BigQuery

Recibe un DataFrame (ya transformado por un source) y lo carga
a la tabla correspondiente en BigQuery.

Modos de escritura:
    - full_refresh → WRITE_TRUNCATE: borra la tabla y la vuelve a crear
    - incremental  → WRITE_APPEND:   agrega filas sin borrar las existentes

Uso:
    from spotify.loaders.bigquery import BigQueryLoader

    loader = BigQueryLoader()
    loader.load(df, table="artists", mode="full_refresh")
"""

import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()


class BigQueryLoader:
    """
    Carga DataFrames a BigQuery usando la librería oficial de Google.

    Cada tabla en BQ corresponde a un source de Spotify:
        sportify_test.artists
        sportify_test.tracks
        sportify_test.albums
        sportify_test.top_tracks
    """

    def __init__(self):
        self.project_id   = os.getenv("GCP_PROJECT_ID")
        self.dataset      = os.getenv("BQ_DATASET")
        credentials_path  = os.getenv("GCP_CREDENTIALS_PATH")

        if not all([self.project_id, self.dataset, credentials_path]):
            raise EnvironmentError(
                "Faltan variables de entorno: GCP_PROJECT_ID, BQ_DATASET o GCP_CREDENTIALS_PATH"
            )

        # Cargamos las credenciales desde el JSON de la Service Account
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        self.client = bigquery.Client(
            project=self.project_id,
            credentials=credentials,
        )

    def load(self, df, table, mode="full_refresh"):
        """
        Carga un DataFrame a una tabla de BigQuery.

        Args:
            df    (pd.DataFrame): Datos a cargar
            table (str):          Nombre de la tabla destino, ej: "artists"
            mode  (str):          "full_refresh" o "incremental"

        Returns:
            None
        """
        if df.empty:
            print(f"[BigQuery] DataFrame vacío — no se carga nada en '{table}'")
            return

        table_id      = f"{self.project_id}.{self.dataset}.{table}"
        write_disposition = self._get_write_disposition(mode)

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            # autodetect=True le dice a BQ que infiera el schema desde el DataFrame
            autodetect=True,
        )

        print(f"[BigQuery] Cargando {len(df)} filas en '{table_id}' (modo: {mode})...")

        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Espera a que el job termine

        # Verificamos cuántas filas quedaron en la tabla
        table_ref  = self.client.get_table(table_id)
        print(f"[BigQuery] Listo. Filas totales en '{table}': {table_ref.num_rows:,}")

    def _get_write_disposition(self, mode):
        """
        Traduce el modo de extracción al write disposition de BigQuery.

        WRITE_TRUNCATE → borra todo y reescribe (full_refresh)
        WRITE_APPEND   → agrega sin borrar (incremental)
        """
        dispositions = {
            "full_refresh": bigquery.WriteDisposition.WRITE_TRUNCATE,
            "incremental":  bigquery.WriteDisposition.WRITE_APPEND,
        }

        if mode not in dispositions:
            raise ValueError(f"Modo inválido: '{mode}'. Usa 'full_refresh' o 'incremental'")

        return dispositions[mode]
