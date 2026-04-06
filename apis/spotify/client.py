"""
client.py — Cliente HTTP genérico para la API de Spotify

Responsabilidad única: hacer peticiones HTTP a Spotify.
No sabe nada de artistas, canciones ni transformaciones.
Solo sabe hablar con la API.
"""

import requests
from spotify.auth import SpotifyAuth


BASE_URL = "https://api.spotify.com/v1"


class SpotifyClient:
    """
    Cliente HTTP reutilizable para todos los sources de Spotify.

    Uso:
        client = SpotifyClient()
        data = client.get("/artists/3TVXtAsR1Inumwj472S9r4")
    """

    def __init__(self):
        self.auth = SpotifyAuth()

    def get(self, endpoint, params=None):
        """
        Hace una petición GET a cualquier endpoint de Spotify.

        Args:
            endpoint (str): Ruta del endpoint, ej: "/artists/123"
            params (dict): Parámetros de query opcionales, ej: {"limit": 10}

        Returns:
            dict: Respuesta de Spotify como diccionario Python
        """
        url = f"{BASE_URL}{endpoint}"

        response = requests.get(
            url=url,
            headers=self.auth.get_auth_header(),
            params=params,
        )

        response.raise_for_status()
        return response.json()
