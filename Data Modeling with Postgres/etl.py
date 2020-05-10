from typing import Any

import numpy as np
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur: Any, filepath: str):
    """
    Processes a single song file.
    @param cur: the database cursor
    @param filepath: the path to the song file
    """
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]]
    song_data = list(song_data.values)
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur: Any, filepath: str):
    """
    Processes a single log file.
    @param cur: the database cursor
    @param filepath: the path to the log file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # convert timestamp column to datetime
    df["ts"] = pd.to_datetime(df["ts"], unit='ms')

    # get all the wanted information from the timestamps
    timestamps = df["ts"].dt.time
    hours = df["ts"].dt.hour
    days = df["ts"].dt.day
    weeks = df["ts"].dt.week
    months = df["ts"].dt.month
    years = df["ts"].dt.year
    weekdays = df["ts"].dt.weekday

    # create a dataframe with the wanted information
    time_df = pd.DataFrame(
        {"timestamp": timestamps, "hour": hours, "day": days, "week": weeks, "month": months, "year": years,
         "weekday": weekdays})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    user_df = user_df.drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur: Any, conn: Any, filepath: str, func: Any):
    """
    Processes either logs or songs depending on the given function.
    @param cur: the database cursor
    @param conn: the database connection
    @param filepath: the path to the data directory
    @param func: the function (process songs or logs)
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def insert_songs_and_logs():
    """
    Inserts songs and logs to our custom database.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    insert_songs_and_logs()
