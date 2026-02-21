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
    timeout=15,
    retries=3,
    sleept_time=1,
    remove_section_headers=True,
)

albums_df = pd.read_csv("albums.csv")

# eras: (Foundation 1981-1989, expansion 1990-1995, mainstream_peak 1996-1999, post_peak 2000-2005, revival 2006-2015, modern 2016-present)
# commercial_peak: (0=underground,1=indie scene,2=major label mid-tier,3=mainstream breakthrough)
