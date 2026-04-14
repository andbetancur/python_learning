"""
auth.py — Manejo de autenticación con la API de Spotify

Flujo: Client Credentials
- No requiere que el usuario inicie sesión
- Sirve para datos públicos: artistas, canciones, álbumes, playlists públicas
- El token dura 60 minutos; esta clase lo renueva automáticamente
"""

import os
import time
import requests
from dotenv import load_dotenv

# Carga las variables del archivo .env al entorno
load_dotenv()


class SpotifyAuth:
    """
    Maneja la autenticación con Spotify usando Client Credentials.

    Uso:
        auth = SpotifyAuth()
        token = auth.get_token()
    """

    # URL donde Spotify entrega los tokens
    TOKEN_URL = "https://accounts.spotify.com/api/token"

    def __init__(self):
        # Lee las credenciales desde el .env
        self.client_id = os.getenv("SPOTIFY_CLIENT_ID")
        self.client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

        # Validar que las credenciales existan
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "Faltan credenciales. Verifica que tu .env tiene "
                "SPOTIFY_CLIENT_ID y SPOTIFY_CLIENT_SECRET"
            )

        # Estado interno del token
        self._token = None
        self._token_expires_at = 0  # Timestamp Unix de cuando vence

    def _request_new_token(self):
        """
        Pide un token nuevo a Spotify.
        Este método es privado (empieza con _), solo lo usa la clase internamente.
        """
        print("Solicitando nuevo token a Spotify...")

        # Spotify espera las credenciales en formato Basic Auth
        # requests las codifica automáticamente con auth=(user, password)
        response = requests.post(
            url=self.TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
        )

        # Lanza un error si Spotify respondió con un código de error (4xx, 5xx)
        response.raise_for_status()

        data = response.json()

        # Guardar el token y calcular cuando vence
        # expires_in viene en segundos (normalmente 3600 = 60 minutos)
        self._token = data["access_token"]
        self._token_expires_at = time.time() + data["expires_in"] - 60
        # Le restamos 60 segundos como margen de seguridad

        print(f"Token obtenido. Vence en {data['expires_in'] // 60} minutos.")

    def get_token(self):
        """
        Retorna un token válido.
        Si el token venció o no existe, pide uno nuevo automáticamente.

        Returns:
            str: El access token para usar en requests a la API
        """
        # time.time() retorna el tiempo actual en segundos (timestamp Unix)
        if self._token is None or time.time() >= self._token_expires_at:
            self._request_new_token()

        return self._token

    def get_auth_header(self):
        """
        Retorna el header de autorización listo para usar en requests.

        Los endpoints de Spotify requieren este header en cada petición:
            Authorization: Bearer <token>

        Returns:
            dict: Header listo para pasar a requests
        """
        return {"Authorization": f"Bearer {self.get_token()}"}


# Este bloque solo se ejecuta cuando corres el archivo directamente
# No se ejecuta cuando otro archivo lo importa
if __name__ == "__main__":
    auth = SpotifyAuth()
    token = auth.get_token()
    print("Token recibido:", token[:30], "...")
    print("Header:", auth.get_auth_header())
