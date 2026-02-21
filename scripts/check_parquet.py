import pandas as pd

df = pd.read_parquet("data/raw/ska_songs_full.parquet")

print("Rows:", len(df))
print("Albums:", df["album"].nunique())
print("Songs per album:")
print(df.groupby("album")["song_title"].count())