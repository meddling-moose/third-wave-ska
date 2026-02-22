import lyricsgenius
import pandas as pd
import time
import os
from dotenv import load_dotenv

load_dotenv()

token = os.getenv("GENIUS_API_TOKEN")
if not token:
    raise RuntimeError("Missing GENIUS_API_TOKEN in .env")

# =============================
# CONFIGURATION
# =============================
GENIUS_API_TOKEN = token

ALBUMS_CSV = "data/raw/albums.csv"
OUTPUT_MASTER = "data/raw/ska_songs_full.parquet"

BATCH_SIZE = 10
BATCH_NUMBER = 0   # Increment to pull songs in batches 1, 2, 3...
SLEEP_SECONDS = 3.0

# If True, you'll see the library's "Searching for..." messages
VERBOSE = True

# =============================
# SETUP
# =============================
genius = lyricsgenius.Genius(
    GENIUS_API_TOKEN,
    timeout=15,
    retries=3,
    remove_section_headers=True,
    skip_non_songs=True,
)
genius.verbose = VERBOSE

albums_df = pd.read_csv(ALBUMS_CSV)

start = BATCH_NUMBER * BATCH_SIZE
end = start + BATCH_SIZE
batch_df = albums_df.iloc[start:end]

song_rows = []
failures = []

print(f"Running batch {BATCH_NUMBER} ({start}–{end})")
print(f"Total albums in batch: {len(batch_df)}")
print("--------------------------------------------------")


def normalize_song_from_track(track):
    """
    lyricsgenius album.tracks can contain:
      - Track objects with .song
      - tuples like (track_number, Song) or (track_number, dict)
      - dicts that include song metadata
      - sometimes Song objects directly
    This returns (song_obj_or_none, song_id_or_none, song_title_guess)
    """
    # Case 1: Track-like object with .song
    if hasattr(track, "song"):
        s = track.song
        return s, getattr(s, "id", None), getattr(s, "title", None)

    # Case 2: tuple
    if isinstance(track, tuple):
        # try each element
        for part in track:
            # Song-like object
            if hasattr(part, "lyrics") or hasattr(part, "title"):
                return part, getattr(part, "id", None), getattr(part, "title", None)
            # dict with song data
            if isinstance(part, dict):
                if "song" in part and isinstance(part["song"], dict):
                    sdict = part["song"]
                    return None, sdict.get("id"), sdict.get("title")
                # sometimes the dict itself is the song dict
                if "id" in part and "title" in part:
                    return None, part.get("id"), part.get("title")

        # common pattern: (track_number, song_dict)
        if len(track) == 2 and isinstance(track[1], dict):
            return None, track[1].get("id"), track[1].get("title")

        return None, None, None

    # Case 3: dict
    if isinstance(track, dict):
        if "song" in track and isinstance(track["song"], dict):
            sdict = track["song"]
            return None, sdict.get("id"), sdict.get("title")
        if "id" in track and "title" in track:
            return None, track.get("id"), track.get("title")

    # Case 4: Song object directly
    if hasattr(track, "lyrics") or hasattr(track, "title"):
        return track, getattr(track, "id", None), getattr(track, "title", None)

    return None, None, None


def get_song_with_lyrics(song_obj, song_id, title_guess, artist_name):
    """
    Ensure we have lyrics. If we only have an id, fetch song by id.
    If we only have a title, fall back to search_song.
    """
    # If we already have a Song and lyrics look populated, keep it.
    if song_obj is not None:
        lyr = getattr(song_obj, "lyrics", "") or ""
        if lyr.strip():
            return song_obj

    # Try by song_id
    if song_id:
        try:
            s = genius.song(song_id)
            if s is not None and getattr(s, "lyrics", ""):
                return s
        except Exception:
            pass

    # Fallback: search by title + artist
    if title_guess:
        try:
            s = genius.search_song(title_guess, artist_name)
            if s is not None and getattr(s, "lyrics", ""):
                return s
        except Exception:
            pass

    return song_obj  # might be None


# =============================
# MAIN LOOP
# =============================
for i, (_, row) in enumerate(batch_df.iterrows(), start=1):
    artist = str(row["artist"]).strip()
    album_name = str(row["album"]).strip()

    print(f"[{i}/{len(batch_df)}] Pulling: {artist} - {album_name}")

    try:
        album = genius.search_album(album_name, artist)
        if album is None:
            failures.append({"artist": artist, "album": album_name, "reason": "album not found"})
            print("   -> Not found.")
            time.sleep(SLEEP_SECONDS)
            continue

        tracks = getattr(album, "tracks", None)
        if not tracks:
            failures.append({"artist": artist, "album": album_name, "reason": "album has no tracks"})
            print("   -> Found album, but no tracks returned.")
            time.sleep(SLEEP_SECONDS)
            continue

        added_any = False

        for track in tracks:
            song_obj, song_id, title_guess = normalize_song_from_track(track)
            song = get_song_with_lyrics(song_obj, song_id, title_guess, artist)

            if song is None:
                continue

            song_rows.append({
                "artist": artist,
                "album": album_name,
                "song_title": getattr(song, "title", title_guess) or title_guess or "",
                "song_id": getattr(song, "id", song_id),
                "lyrics": getattr(song, "lyrics", "") or "",
                "year": row.get("year"),
                "era": row.get("era"),
                "city": row.get("city"),
                "state": row.get("state"),
                "label": row.get("label"),
                "label_type": row.get("label_type"),
                "commercial_peak": row.get("commercial_peak"),
            })
            added_any = True

        if not added_any:
            failures.append({"artist": artist, "album": album_name, "reason": "no songs extracted from tracks"})
            print("   -> No songs extracted from this album's tracks.")

        time.sleep(SLEEP_SECONDS)

    except Exception as e:
        print(f"   -> Error: {e}")
        failures.append({"artist": artist, "album": album_name, "reason": f"exception: {repr(e)}"})
        time.sleep(SLEEP_SECONDS)
        continue


# =============================
# SAVE RESULTS
# =============================
songs_df = pd.DataFrame(song_rows)

RAW_DIR = "data/raw"
BATCH_DIR = os.path.join(RAW_DIR, "batch")
os.makedirs(BATCH_DIR, exist_ok=True)

batch_parquet = os.path.join(BATCH_DIR, f"ska_songs_batch_{BATCH_NUMBER}.parquet")
failures_csv = os.path.join(BATCH_DIR, f"failures_batch_{BATCH_NUMBER}.csv")

# Always store the master inside data/raw/
master_parquet = os.path.join(RAW_DIR, "ska_songs_full.parquet")

if not songs_df.empty:
    # Save the batch output
    songs_df.to_parquet(batch_parquet, index=False)

    # Append to master safely
    if os.path.exists(master_parquet):
        existing = pd.read_parquet(master_parquet)
        combined = pd.concat([existing, songs_df], ignore_index=True)
    else:
        combined = songs_df.copy()

    # -----------------------------
    # SAFE DEDUPE (handles null song_id)
    # -----------------------------
    if "song_id" in combined.columns:
        with_id = combined[combined["song_id"].notna()].drop_duplicates(
            subset=["song_id"], keep="first"
        )
        no_id = combined[combined["song_id"].isna()].drop_duplicates(
            subset=["artist", "album", "song_title"], keep="first"
        )
        combined = pd.concat([with_id, no_id], ignore_index=True)
    else:
        combined = combined.drop_duplicates(
            subset=["artist", "album", "song_title"], keep="first"
        )

    combined.to_parquet(master_parquet, index=False)

    print("--------------------------------------------------")
    print(f"Saved {len(songs_df)} songs to {batch_parquet}")
    print(f"Master now has {len(combined)} songs at {master_parquet}")
else:
    print("--------------------------------------------------")
    print("Saved 0 songs (songs_df empty).")

if failures:
    pd.DataFrame(failures).to_csv(failures_csv, index=False)
    print(f"Failures saved to {failures_csv}")
    print("Albums with issues in this batch:")
    for f in failures:
        print(f"  - {f['artist']} - {f['album']} ({f['reason']})")

print("Done.")

# Notes on data columns
# eras: (Foundation 1981-1989, expansion 1990-1995, mainstream_peak 1996-1999, post_peak 2000-2005, revival 2006-2015, modern 2016-present)
# commercial_peak: (0=underground,1=indie scene,2=major label mid-tier,3=mainstream breakthrough)
