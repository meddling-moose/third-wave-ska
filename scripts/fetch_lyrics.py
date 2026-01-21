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
    ("The Mighty Mighty Bosstones", "Someday I Suppose"),
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
    ("Catch 22", "Kaesbey Nights"),
    ("Catch 22", "9mm and a Three Piece Suit"),
    ("Operation Ivy", "Sound System"),
    ("Reel Big Fish", "Beer"),
    ("The O.C. Supertones", "Supertones Strike Back"),
    ("Sublime", "Badfish"),
    ("Goldfinger", "Tijuana Sunrise"),
    ("No Doubt", "Spiderwebs"),
    ("No Doubt", "Total Hate '95"),
    ("Rancid", "Time Bomb"),
    ("Rancid", "Ruby Soho"),
    ("The Toasters", "2Tone Army"),
    ("The Slackers", "Watch This"),
    ("Big D and the Kids Table", "Noise Complaint"),
    ("Big D and the Kids Table", "Shining On"),
    ("No Doubt", "Doormat"),
    ("Mad Caddies", "Road Rash"),
    ("The Hippos", "Lost It"),
    ("The Hippos", "Far Behind"),
    ("Mustard Plug", "Mr. Smiley"),
    ("Dance Hall Crashers", "Lost Again"),
    ("The Pietasters", "Out All Night"),
    ("Less Than Jake", "Johnny Quest Thinks We're Sellouts"),
    ("Buck-O-Nine", "My Town"),
    ("The Suicide Machines", "New Girl"),
    ("Sublime", "Seed"),
    ("The Interrupters", "Bad Guy"),
    ("Five Iron Frenzy", "Get Your Riot Gear"),
    ("Mustard Plug", "Beer"),
    ("MU330", "Stuff"),
    ("Slapstick", "There's a Metal Head in The Parking Lot"),
    ("Skankin Pickle", "Special Brew"),
    ("The Arrogant Sons of Bitches", "So Lets Go Nowhere"),
    ("Choking Victim", "500 Channels"),
    ("Fishbone", "Party at Ground Zero"),
    ("NOFX", "Linoleum"),
    ("NOFX", "All Outta Angst"),
    ("100 Gecs", "I Got My Tooth Removed"),
    ("The Interrupters", "She's Kerosene"),
    ("Streetlight Manifesto", "Such Great Heights"),
    ("Streetlight Manifesto", "Me and Julio Down By The Schoolyard"),
    ("[spunge]", "Kicking Pigeons")
]

rows = []

print(f"Total Number of Songs: {len(songs)}")

for artist, title in songs:
    missed_songs = 0
    print(f"Fetching: {artist} - {title}")
    song = genius.search_song(title, artist)

    if not song or not song.lyrics:
        missed_songs += 1
        print(" -> not found")
        continue

    rows.append({
        "artist": artist,
        "song_title": title,
        "album": getattr(song, "album", None),
        "release_year": getattr(song, "year", None),
        "lyrics": song.lyrics,
        "source": "genius"
    })
print(f"Total Missed Songs: {missed_songs}")

df = pd.DataFrame(rows)
outpath = "data/raw/lyrics_raw.csv"
df.to_csv(outpath, index=False)
print(f"Wrote {len(df)} rows to outpath")