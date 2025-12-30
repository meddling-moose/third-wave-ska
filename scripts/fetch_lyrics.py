import os
import pandas as pd
import lyricsgenius
from dotenv import load_dotenv

load_dotenv()

token = os.getenv("GENIUS_API_TOKEN")
if not token:
    raise RuntimeError("Missing GENIUS_API_TOKEN in .env")

genius = lyricsgenius.Genius(
    token,
    skip_non_songs=True,
    remove_section_headers=True,
    timeout=20,
    retries=2,
)

songs = [
    ("The Mighty Mighty Bosstones", "The Impression That I Get"),
    ("The Mighty Might Bosstones", "Someday I Suppose"),
    ("Goldfinger", "Here in Your Bedroom"),
    ("The Suicide Machines", "No Face"),
    ("No Doubt", "Sunday Morning"),
    ("Buck-O-Nine", "My Town"),
    ("Sublime", "Wrong Way"),
    ("The Mighty Mighty Bosstones", "Rascal King"),
    ("Goldfinger", "Lonely Place"),
    ("Save Ferris", "Come On Eileen"),
    ("The Mighty Mighty Bosstones", "Royal Oil"),
    ("Save Ferris", "Goodbye"),
    ("The Specials", "It's You"),
    ("Less Than Jake", "History of a Boring Town"),
    ("Less Than Jake", "All My Best Friends Are Metalheads"),
    ("Reel Big Fish", "Sell Out"),
    ("Goldfinger", "Superman"),
    ("Streetlight Manifesto", "Everything Went Numb"),
    ("Sublime", "Santeria"),
    ("Reel Big Fish", "Take On Me"),
    ("The Aquabats", "Super Rad"),
    ("Less Than Jake", "The Science of Selling Yourself Short"),
    ("Streetlight Manifesto", "Point / Counterpoint"),
    
]

rows = []

for artist, title in songs:
    print(f"Fetching: {artist} - {title}")
    song = genius.search_song(title, artist)

    if not song or not song.lyrics:
        print(" -> not fouond")
        continue

    rows.append({
        "artist": artist,
        "song_title": title,
        "album": getattr(song, "album", None),
        "release_year": getattr(song, "year", None),
        "lyrics": song.lyrics,
        "source": "genius"
    })

df = pd.DataFrame(rows)
outpath = "data/raw/lyrics_raw.csv"
df.to_csv(outpath, index=False)
print(f"/nWrote {len(df)} rows to outpath")