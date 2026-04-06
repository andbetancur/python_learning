"""
albums.py — Source de Álbumes de Spotify

Extrae información de álbumes dado una lista de IDs.

Uso:
    from spotify.sources.albums import AlbumsSource

    source = AlbumsSource()
    df = source.extract(ids=["album_id1", "album_id2"], mode="full_refresh")
"""

import pandas as pd
from spotify.sources.base import BaseSource


class AlbumsSource(BaseSource):

    source_name = "albums"

    def _fetch_records(self, ids):
        """
        Llama al endpoint /albums/{id} individualmente por cada álbum.
        (Spotify restringió el endpoint batch /albums?ids=... en 2024)
        """
        records = []

        for album_id in ids:
            data = self.client.get(f"/albums/{album_id}")
            records.append(data)
            print(f"  Álbum obtenido: {data['name']}")

        return records

    def _to_dataframe(self, records):
        rows = []
        for album in records:
            if album is None:
                continue
            rows.append({
                "album_id":             album["id"],
                "name":                 album["name"],
                "album_type":           album["album_type"],
                "total_tracks":         album["total_tracks"],
                "release_date":         album["release_date"],
                "release_date_precision": album["release_date_precision"],
                "artist_id":            album["artists"][0]["id"],
                "artist_name":          album["artists"][0]["name"],
                "all_artists":          ", ".join(a["name"] for a in album["artists"]),
                "image_url":            album["images"][0]["url"] if album["images"] else None,
                "spotify_url":          album["external_urls"]["spotify"],
                "uri":                  album["uri"],
            })

        return pd.DataFrame(rows)
