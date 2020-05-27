import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

load_dotenv()

DB_USER = os.getenv("DB_USER", default="OOPS")
DB_PASSWORD = os.getenv("DB_PASSWORD", default="OOPS")
DB_NAME = os.getenv("DB_NAME", default="OOPS")
DB_HOST = os.getenv("DB_HOST", default="OOPS")
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
                        host=DB_HOST)
c = conn.cursor()

spotify_table = """
  CREATE TABLE IF NOT EXISTS spotify_gal (
    song_name VARCHAR(50) NOT NULL,
    song_popularity INT NOT NULL,
    song_duration_ms INT NOT NULL,
    acousticness FLOAT NOT NULL,
    danceability FLOAT NOT NULL,
    energy FLOAT NOT NULL,
    instrumentalness FLOAT NOT NULL,
    key FLOAT NOT NULL,
    liveness FLOAT NOT NULL,
    loudness FLOAT NOT NULL,
    audio_mode FLOAT NOT NULL,
    speechiness FLOAT NOT NULL,
    tempo FLOAT NOT NULL,
    time_signature INT NOT NULL,
    audio_valence FLOAT NOT NULL
  );
"""

c.execute(spotify_table)
conn.commit()

data = pd.read_csv("song_data.csv", error_bad_lines=False)

data.to_csv("song_data_clean.csv", index=False)


#data.to_sql("spotify_gal", conn)





#with open("song_data.csv", 'r') as Csv:
#    next(Csv)
#    c.copy_from(Csv, 'spotify_gal', sep=',')
#

#
for row in data.itertuples():
    c.execute('''
          INSERT INTO spotify_gal VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
          %s,%s,%s);
          ''',
          (row.song_name,
          row.song_popularity,
          row.song_duration_ms,
          row.acousticness,
          row.danceability,
          row.energy,
          row.instrumentalness,
          row.key,
          row.liveness,
          row.loudness,
          row.audio_mode,
          row.speechiness,
          row.tempo,
          row.time_signature,
          row.audio_valence)
          )
conn.commit()



