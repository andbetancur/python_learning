"""
extractor.py — Extracción de datos desde la API de Spotify

Cada método hace una petición a un endpoint específico de Spotify
y retorna los datos crudos en formato JSON (diccionarios de Python).

La transformación a DataFrame ocurre en transformers.py
"""

import requests
from spotify.auth import SpotifyAuth


# URL base de todos los endpoints de Spotify
BASE_URL = "https://api.spotify.com/v1"


class SpotifyExtractor:
    """
    Extrae datos crudos de la API de Spotify.

    Uso:
        extractor = SpotifyExtractor()
        artist = extractor.get_artist("3TVXtAsR1Inumwj472S9r4")  # ID de Drake
    """

    def __init__(self):
        # Reutilizamos SpotifyAuth — ella maneja el token automáticamente
        self.auth = SpotifyAuth()

    def _get(self, endpoint, params=None):
        """
        Método interno que hace todas las peticiones GET a Spotify.

        Centralizar las peticiones aquí tiene ventajas:
        - Si Spotify cambia algo, solo editamos un lugar
        - El manejo de errores está en un solo lugar
        - El header de autenticación se agrega automáticamente

        Args:
            endpoint (str): La ruta del endpoint, ej: "/artists/123"
            params (dict): Parámetros opcionales de la URL, ej: {"limit": 10}

        Returns:
            dict: La respuesta de Spotify en formato diccionario
        """
        url = f"{BASE_URL}{endpoint}"

        response = requests.get(
            url=url,
            headers=self.auth.get_auth_header(),  # Token agregado automáticamente
            params=params,                         # Se convierten en ?limit=10&... en la URL
        )

        # Si Spotify responde con error (401, 404, 429, etc.) lanza una excepción
        response.raise_for_status()

        return response.json()

    # -------------------------------------------------------------------------
    # ARTISTAS
    # -------------------------------------------------------------------------

    def get_artist(self, artist_id):
        """
        Obtiene la información de un artista por su ID de Spotify.

        ¿Cómo encontrar el ID de un artista?
        En Spotify desktop: clic derecho en artista → Compartir → Copiar enlace
        El ID es la parte final de la URL:
        https://open.spotify.com/artist/3TVXtAsR1Inumwj472S9r4
                                                 ^^^^^^^^^^^^^^^^^^^^^^^^

        Args:
            artist_id (str): ID de Spotify del artista

        Returns:
            dict: Datos completos del artista
        """
        return self._get(f"/artists/{artist_id}")

    def get_artist_top_tracks(self, artist_id, market="US"):
        """
        Obtiene las top 10 canciones de un artista.

        Args:
            artist_id (str): ID del artista
            market (str): País para el que se obtienen las canciones (código ISO)
                         Ej: "US", "CO", "MX", "ES"

        Returns:
            list: Lista de diccionarios, cada uno representa una canción
        """
        data = self._get(
            f"/artists/{artist_id}/top-tracks",
            params={"market": market}
        )
        # La API devuelve {"tracks": [...]} — nosotros retornamos solo la lista
        return data["tracks"]

    def get_artist_albums(self, artist_id, limit=20):
        """
        Obtiene los álbumes de un artista.

        Args:
            artist_id (str): ID del artista
            limit (int): Cuántos álbumes traer (máximo 50)

        Returns:
            list: Lista de álbumes
        """
        data = self._get(
            f"/artists/{artist_id}/albums",
            params={
                "limit": limit,
                "include_groups": "album,single"  # Excluye compilaciones y apariciones
            }
        )
        return data["items"]

    # -------------------------------------------------------------------------
    # BÚSQUEDA
    # -------------------------------------------------------------------------

    def search(self, query, search_type="artist", limit=10):
        """
        Busca artistas, canciones, álbumes o playlists en Spotify.

        Args:
            query (str): Texto a buscar, ej: "Bad Bunny"
            search_type (str): Qué buscar — "artist", "track", "album", "playlist"
            limit (int): Cuántos resultados traer (máximo 50)

        Returns:
            list: Lista de resultados
        """
        data = self._get(
            "/search",
            params={
                "q": query,
                "type": search_type,
                "limit": limit
            }
        )

        # La API devuelve la clave en plural: "artists", "tracks", "albums"
        key = f"{search_type}s"
        return data[key]["items"]

    # -------------------------------------------------------------------------
    # CANCIONES (TRACKS)
    # -------------------------------------------------------------------------

    def get_track(self, track_id):
        """
        Obtiene la información de una canción por su ID.

        Args:
            track_id (str): ID de Spotify de la canción

        Returns:
            dict: Datos completos de la canción
        """
        return self._get(f"/tracks/{track_id}")

    def get_album_tracks(self, album_id, limit=50):
        """
        Obtiene todas las canciones de un álbum.

        Args:
            album_id (str): ID del álbum
            limit (int): Cuántas canciones traer (máximo 50)

        Returns:
            list: Lista de canciones del álbum
        """
        data = self._get(
            f"/albums/{album_id}/tracks",
            params={"limit": limit}
        )
        return data["items"]


# Prueba directa del módulo
if __name__ == "__main__":
    extractor = SpotifyExtractor()

    # Buscamos a Bad Bunny
    print("=== Búsqueda de artista ===")
    resultados = extractor.search("Bad Bunny", search_type="artist", limit=1)
    artista = resultados[0]

    print(f"Nombre: {artista['name']}")
    print(f"ID: {artista['id']}")
    print(f"Seguidores: {artista['followers']['total']:,}")
    print(f"Popularidad: {artista['popularity']}/100")
    print(f"Géneros: {artista['genres']}")

    print("\n=== Top tracks ===")
    top_tracks = extractor.get_artist_top_tracks(artista["id"], market="US")
    for i, track in enumerate(top_tracks[:5], start=1):
        print(f"{i}. {track['name']} — popularidad: {track['popularity']}")
