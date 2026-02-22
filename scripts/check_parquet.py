import pandas as pd

df = pd.read_parquet("data/raw/ska_songs_full.parquet")

print("Rows:", len(df))
print("Albums:", df["album"].nunique())

songs_per_album = df.groupby("album")["song_title"].count()

print("Songs per album:")
print(songs_per_album)

print(f"Average songs per album: {songs_per_album.mean()}")