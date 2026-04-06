from spotify.sources.artists import ArtistsSource
from spotify.loaders.bigquery import BigQueryLoader

# IDs de prueba (Bad Bunny y Drake)
artist_ids = [
  "4q3ewBCX7sLwd24euuV69X",  # Bad Bunny
  "3TVXtAsR1Inumwj472S9r4",  # Drake
]

# 1. Extraemos de Spotify
source = ArtistsSource()
df = source.extract(ids=artist_ids, mode="full_refresh")

print("\nDataFrame extraído:")
print(df)

# 2. Cargamos a BigQuery
loader = BigQueryLoader()
loader.load(df, table="artists", mode="full_refresh")