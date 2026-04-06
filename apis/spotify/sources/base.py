"""
base.py — Clase base para todos los sources de Spotify

Define el contrato que deben cumplir todos los sources:
- Método extract() que acepta mode="full_refresh" o mode="incremental"
- Columna process_date en todos los DataFrames resultantes
- Integración automática con StateManager para incrementales

Cada source hijo (artists.py, tracks.py, etc.) hereda de aquí
y solo necesita implementar _fetch_records() y _to_dataframe().
"""

from datetime import date
from abc import ABC, abstractmethod
import pandas as pd
from spotify.client import SpotifyClient
from spotify.state import StateManager


class BaseSource(ABC):
    """
    Clase base abstracta para todos los sources de Spotify.

    ABC = Abstract Base Class: una clase que no se puede instanciar
    directamente, solo sirve como molde para otras clases.

    Cómo crear un nuevo source:
        class MySource(BaseSource):
            source_name = "my_source"

            def _fetch_records(self, ids):
                # lógica de extracción
                ...

            def _to_dataframe(self, records):
                # lógica de transformación
                ...
    """

    # Cada subclase debe definir su nombre (se usa en el estado)
    source_name = None

    def __init__(self):
        if not self.source_name:
            raise NotImplementedError("El source debe definir 'source_name'")

        self.client = SpotifyClient()
        self.state = StateManager()

    def extract(self, ids, mode="full_refresh"):
        """
        Punto de entrada principal. Extrae datos según el modo elegido.

        Args:
            ids (list): Lista de IDs de Spotify a extraer
            mode (str): "full_refresh" o "incremental"

        Returns:
            pd.DataFrame: Datos extraídos con columna process_date
        """
        if mode == "full_refresh":
            return self._full_refresh(ids)
        elif mode == "incremental":
            return self._incremental(ids)
        else:
            raise ValueError(f"Modo inválido: '{mode}'. Usa 'full_refresh' o 'incremental'")

    def _full_refresh(self, ids):
        """
        Extrae todos los IDs sin importar si ya fueron procesados.
        Sobreescribe cualquier estado anterior.
        """
        print(f"[{self.source_name}] Modo: FULL REFRESH — procesando {len(ids)} IDs")

        records = self._fetch_records(ids)
        df = self._to_dataframe(records)
        df = self._add_process_date(df)

        # Actualiza el estado con todos los IDs procesados
        self.state.update(self.source_name, ids)

        print(f"[{self.source_name}] Extraídos {len(df)} registros.")
        return df

    def _incremental(self, ids):
        """
        Solo extrae IDs que no hayan sido procesados anteriormente.
        """
        processed_ids = self.state.get_processed_ids(self.source_name)
        new_ids = [i for i in ids if i not in processed_ids]

        if not new_ids:
            print(f"[{self.source_name}] Modo: INCREMENTAL — sin registros nuevos.")
            return pd.DataFrame()  # DataFrame vacío

        print(f"[{self.source_name}] Modo: INCREMENTAL — {len(new_ids)} nuevos de {len(ids)} totales")

        records = self._fetch_records(new_ids)
        df = self._to_dataframe(records)
        df = self._add_process_date(df)

        # Solo actualiza el estado con los IDs nuevos
        self.state.update(self.source_name, new_ids)

        print(f"[{self.source_name}] Extraídos {len(df)} registros nuevos.")
        return df

    def _add_process_date(self, df):
        """
        Agrega la columna process_date con la fecha actual de extracción.
        Se aplica a todos los DataFrames antes de retornarlos.
        """
        df["process_date"] = date.today()
        return df

    @abstractmethod
    def _fetch_records(self, ids):
        """
        Llama a la API de Spotify y retorna los datos crudos.
        Cada subclase implementa esto según su endpoint.

        Returns:
            list: Lista de diccionarios con los datos crudos de Spotify
        """
        ...

    @abstractmethod
    def _to_dataframe(self, records):
        """
        Transforma la lista de registros crudos en un DataFrame limpio.
        Cada subclase define qué campos extraer y cómo nombrarlos.

        Returns:
            pd.DataFrame
        """
        ...
