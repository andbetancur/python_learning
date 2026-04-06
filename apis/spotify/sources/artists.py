"""
artists.py — Source de Artistas de Spotify

Extrae información de artistas dado una lista de IDs.

Uso:
    from spotify.sources.artists import ArtistsSource

    source = ArtistsSource()

    # Full refresh: extrae todos los IDs
    df = source.extract(ids=["id1", "id2"], mode="full_refresh")

    # Incremental: solo extrae IDs nuevos
    df = source.extract(ids=["id1", "id2", "id3"], mode="incremental")
"""

import pandas as pd
from spotify.sources.base import BaseSource


class ArtistsSource(BaseSource):

    source_name = "artists"

    def _fetch_records(self, ids):
        """
        Llama al endpoint /artists/{id} individualmente por cada artista.
        (Spotify restringió el endpoint batch /artists?ids=... en 2024)
        """
        records = []

        for artist_id in ids:
            data = self.client.get(f"/artists/{artist_id}")
            records.append(data)
            print(f"  Artista obtenido: {data['name']}")

        return records

    def _to_dataframe(self, records):
        """
        Transforma la lista de artistas crudos en un DataFrame limpio.
        Nota: desde 2024 Spotify ya no retorna popularity, followers ni genres
        en endpoints públicos con Client Credentials.
        """
        rows = []
        for artist in records:
            if artist is None:
                continue
            rows.append({
                "artist_id":   artist["id"],
                "name":        artist["name"],
                "image_url":   artist["images"][0]["url"] if artist["images"] else None,
                "spotify_url": artist["external_urls"]["spotify"],
                "uri":         artist["uri"],
            })

        return pd.DataFrame(rows)
