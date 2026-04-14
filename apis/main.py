"""
main.py — Orquestador principal del pipeline de Spotify

Aquí defines:
- Qué artistas/canciones/álbumes extraer
- Si correr en full_refresh o incremental
- Qué hacer con los DataFrames resultantes

Por ahora imprimimos y guardamos como CSV.
Después conectaremos esto a BigQuery.
"""

from spotify.sources.artists import ArtistsSource
from spotify.sources.top_tracks import TopTracksSource
from spotify.sources.albums import AlbumsSource
from spotify.sources.tracks import TracksSource

# =============================================================================
# CONFIGURACIÓN — edita aquí qué quieres extraer
# =============================================================================

# IDs de artistas a extraer (copia desde Spotify: clic derecho → Compartir → enlace)
ARTIST_IDS = [
    "3TVXtAsR1Inumwj472S9r4",  # Drake
    "4q3ewBCX7sLwd24euuV69X",  # Bad Bunny
    "1Xyo4u8uXC1ZmMpatF05PJ",  # The Weeknd
]

# Modo de extracción: "full_refresh" o "incremental"
MODE = "full_refresh"

# =============================================================================
# PIPELINE
# =============================================================================

def run():
    print("=" * 60)
    print(f"Iniciando pipeline Spotify | Modo: {MODE.upper()}")
    print("=" * 60)

    # --- 1. Artistas ---
    print("\n--- Extrayendo ARTISTAS ---")
    artists_df = ArtistsSource().extract(ids=ARTIST_IDS, mode=MODE)
    print(artists_df[["artist_id", "name", "spotify_url", "process_date"]].to_string())

    # --- 2. Top Tracks por artista ---
    print("\n--- Extrayendo TOP TRACKS ---")
    top_tracks_df = TopTracksSource(albums_limit=2).extract(ids=ARTIST_IDS, mode=MODE)
    print(top_tracks_df[["artist_id", "name", "album_name", "release_date", "process_date"]].to_string())

    # --- 3. Álbumes de esos artistas ---
    print("\n--- Extrayendo ÁLBUMES ---")
    # Primero obtenemos los IDs de álbumes desde los top tracks
    album_ids = top_tracks_df["album_id"].unique().tolist()
    albums_df = AlbumsSource().extract(ids=album_ids, mode=MODE)
    print(albums_df[["name", "artist_name", "release_date", "total_tracks", "process_date"]].to_string())

    print("\n" + "=" * 60)
    print("Pipeline finalizado.")
    print(f"  Artistas:   {len(artists_df)} filas")
    print(f"  Top Tracks: {len(top_tracks_df)} filas")
    print(f"  Álbumes:    {len(albums_df)} filas")
    print("=" * 60)

    return {
        "artists":    artists_df,
        "top_tracks": top_tracks_df,
        "albums":     albums_df,
    }


if __name__ == "__main__":
    dataframes = run()
