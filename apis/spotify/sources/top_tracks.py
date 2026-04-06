"""
top_tracks.py — Source de Tracks por Artista

Dado una lista de IDs de artistas, extrae los tracks de sus álbumes más recientes.

Estrategia (el endpoint /top-tracks fue restringido por Spotify en 2024):
    1. Obtenemos los álbumes del artista con /artists/{id}/albums
    2. Tomamos los primeros N álbumes
    3. Obtenemos los tracks de cada álbum con /albums/{id}/tracks

Uso:
    from spotify.sources.top_tracks import TopTracksSource

    source = TopTracksSource(albums_limit=3)
    df = source.extract(ids=["artist_id1", "artist_id2"], mode="full_refresh")
"""

import pandas as pd
from spotify.sources.base import BaseSource


class TopTracksSource(BaseSource):

    source_name = "top_tracks"

    def __init__(self, albums_limit=3):
        """
        Args:
            albums_limit (int): Cuántos álbumes recientes extraer por artista
        """
        super().__init__()
        self.albums_limit = albums_limit

    def _fetch_records(self, artist_ids):
        """
        Para cada artista:
        1. Obtiene sus álbumes más recientes
        2. Obtiene los tracks de cada álbum
        """
        records = []

        for artist_id in artist_ids:
            # Paso 1: Álbumes del artista
            albums_data = self.client.get(
                f"/artists/{artist_id}/albums",
                params={
                    "limit": self.albums_limit,
                    "include_groups": "album,single",
                }
            )
            albums = albums_data.get("items", [])
            print(f"  Artista {artist_id}: {len(albums)} álbumes encontrados")

            # Paso 2: Tracks de cada álbum
            for album in albums:
                tracks_data = self.client.get(
                    f"/albums/{album['id']}/tracks",
                    params={"limit": 50}
                )
                for track in tracks_data.get("items", []):
                    # Enriquecemos cada track con info del artista y álbum
                    track["_source_artist_id"] = artist_id
                    track["_album_id"] = album["id"]
                    track["_album_name"] = album["name"]
                    track["_release_date"] = album["release_date"]
                    track["_album_type"] = album["album_type"]

                records.extend(tracks_data.get("items", []))

        return records

    def _to_dataframe(self, records):
        rows = []
        for track in records:
            rows.append({
                "artist_id":    track["_source_artist_id"],
                "track_id":     track["id"],
                "name":         track["name"],
                "duration_ms":  track["duration_ms"],
                "duration_min": round(track["duration_ms"] / 60000, 2),
                "explicit":     track["explicit"],
                "track_number": track["track_number"],
                "disc_number":  track["disc_number"],
                "is_local":     track["is_local"],
                "album_id":     track["_album_id"],
                "album_name":   track["_album_name"],
                "album_type":   track["_album_type"],
                "release_date": track["_release_date"],
                "all_artists":  ", ".join(a["name"] for a in track["artists"]),
                "spotify_url":  track["external_urls"]["spotify"],
                "uri":          track["uri"],
            })

        return pd.DataFrame(rows)
