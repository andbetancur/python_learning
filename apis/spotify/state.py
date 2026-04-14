"""
state.py — Manejo del estado para extracciones incrementales

Guarda y lee un archivo .state.json que registra:
- Cuándo se corrió cada source por última vez
- Qué IDs ya fueron procesados

Esto permite que el modo incremental sepa qué ya extrajo
y solo procese lo nuevo.
"""

import json
import os
from datetime import datetime

STATE_FILE = ".state.json"


class StateManager:
    """
    Lee y escribe el estado de las extracciones en un archivo JSON local.

    Estructura del archivo .state.json:
    {
        "artists": {
            "last_run": "2026-04-01T10:00:00",
            "processed_ids": ["id1", "id2", ...]
        },
        "tracks": {
            "last_run": "2026-04-01T10:00:00",
            "processed_ids": ["id3", "id4", ...]
        }
    }
    """

    def __init__(self):
        self._state = self._load()

    def _load(self):
        """Lee el archivo de estado. Si no existe, retorna un dict vacío."""
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        return {}

    def _save(self):
        """Escribe el estado actual al archivo JSON."""
        with open(STATE_FILE, "w") as f:
            json.dump(self._state, f, indent=2, default=str)

    def get_processed_ids(self, source_name):
        """
        Retorna el set de IDs ya procesados para un source.

        Args:
            source_name (str): Nombre del source, ej: "artists"

        Returns:
            set: IDs ya procesados (vacío si es la primera vez)
        """
        return set(self._state.get(source_name, {}).get("processed_ids", []))

    def get_last_run(self, source_name):
        """
        Retorna la fecha/hora del último run de un source.

        Returns:
            str | None: Timestamp del último run, o None si nunca corrió
        """
        return self._state.get(source_name, {}).get("last_run")

    def update(self, source_name, new_ids):
        """
        Actualiza el estado después de una extracción exitosa.

        Args:
            source_name (str): Nombre del source
            new_ids (list): IDs procesados en esta extracción
        """
        existing_ids = self.get_processed_ids(source_name)
        all_ids = list(existing_ids | set(new_ids))  # Unión de sets

        self._state[source_name] = {
            "last_run": datetime.now().isoformat(),
            "processed_ids": all_ids,
        }

        self._save()
        print(f"[Estado] '{source_name}' actualizado. Total IDs procesados: {len(all_ids)}")

    def reset(self, source_name):
        """
        Borra el estado de un source (útil para forzar un full refresh).

        Args:
            source_name (str): Nombre del source a resetear
        """
        if source_name in self._state:
            del self._state[source_name]
            self._save()
            print(f"[Estado] '{source_name}' reseteado.")
