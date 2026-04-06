"""
tracks.py — Source de Canciones (Tracks) de Spotify

Extrae información de canciones dado una lista de IDs.

Uso:
    from spotify.sources.tracks import TracksSource

    source = TracksSource()
    df = source.extract(ids=["track_id1", "track_id2"], mode="full_refresh")
"""

import pandas as pd
from spotify.sources.base import BaseSource


class TracksSource(BaseSource):

    source_name = "tracks"

    def _fetch_records(self, ids):
        """
        Llama al endpoint /tracks con hasta 50 IDs a la vez.
        """
        records = []

        batch_size = 50
        for i in range(0, len(ids), batch_size):
            batch = ids[i : i + batch_size]
            ids_str = ",".join(batch)

            data = self.client.get("/tracks", params={"ids": ids_str})
            records.extend(data["tracks"])

            print(f"  Lote {i // batch_size + 1}: {len(batch)} tracks obtenidos")

        return records

    def _to_dataframe(self, records):
        rows = []
        for track in records:
            if track is None:
                continue
            rows.append({
                "track_id":        track["id"],
                "name":            track["name"],
                "duration_ms":     track["duration_ms"],
                "duration_min":    round(track["duration_ms"] / 60000, 2),  # ms → minutos
                "popularity":      track["popularity"],
                "explicit":        track["explicit"],       # True/False
                "track_number":    track["track_number"],   # Posición en el álbum
                "disc_number":     track["disc_number"],
                "preview_url":     track.get("preview_url"),  # Puede ser null
                "album_id":        track["album"]["id"],
                "album_name":      track["album"]["name"],
                "album_type":      track["album"]["album_type"],  # album, single, compilation
                "release_date":    track["album"]["release_date"],
                "artist_id":       track["artists"][0]["id"],      # Artista principal
                "artist_name":     track["artists"][0]["name"],
                "all_artists":     ", ".join(a["name"] for a in track["artists"]),
                "spotify_url":     track["external_urls"]["spotify"],
            })

        return pd.DataFrame(rows)
