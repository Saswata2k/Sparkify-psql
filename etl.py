import glob
import os
import traceback

import pandas as pd
import psycopg2

from sql_queries import *


def process_song_file(cur, filepath):
    """
     Reads song json files and inserts into T_SONG table
    """
    df_song = pd.read_json(filepath, lines=True)
    # insert song record
    song_data = list(df_song[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    try:
        cur.execute(song_table_insert, song_data)
    except Exception as e:
        print("Error occurred during insert to T_SONG " + str(e))
        print(traceback.format_exc())

    # insert artist record
    artist_data = list(df_song[['artist_id', 'artist_latitude', 'artist_location',
                                'artist_longitude', 'artist_name']].values[0])
    try:
        cur.execute(artist_table_insert, artist_data)
    except Exception as e:
        print("Error occurred during insert to T_ARTIST " + str(e))
        print(traceback.format_exc())


def process_log_file(cur, filepath):
    """
         - Reads log json files
         - Delegates log data to separate functions for inserting to tables
    """
    df_logs = pd.read_json(filepath, lines=True)
    # filter by NextSong action
    df_logs = df_logs[df_logs['page'] == 'NextSong']

    # convert timestamp column to datetime
    load_time_table(cur, df_logs)

    load_user_table(cur, df_logs)

    load_song_play_table(cur, df_logs)


def load_song_play_table(cur, df_logs):
    """
        Reads from log dataframe and inserts into T_SONG_PLAY table
    """
    df_song_play = df_logs[['song', 'artist', 'length', 'ts', 'userId', 'level', 'location', 'userAgent', 'sessionId']]
    df_song_play.reset_index(drop=True, inplace=True)
    # insert song-play records
    count = 0
    for index, row in df_song_play.iterrows():
        try:
            # get song_id and artist_id from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()

            if results:
                song_id, artist_id = results
            else:
                song_id, artist_id = None, None

            # insert songplay record
            row_tmsp = pd.to_datetime(row.ts, unit='ms').strftime("%Y-%m-%d %H:%M:%S")
            # insert songplay record
            song_play_data = (
                row_tmsp, row.userId, row.level, song_id, artist_id, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, song_play_data)
            count += 1
        except Exception as e:
            print("Error occurred during insert to T_SONG_PLAY " + str(e))
            print(traceback.format_exc())
    print(f'Total {count} records inserted successfully')


def load_time_table(cur, df_logs):
    """
        Reads time data from log dataframe and inserts into T_TIME_TABLE table
    """
    col_tmsp = pd.to_datetime(df_logs['ts'], unit='ms')
    col_start_time = col_tmsp.dt.strftime("%Y-%m-%d %H:%M:%S")
    col_day = col_tmsp.dt.day
    col_week = col_tmsp.dt.week
    col_month = col_tmsp.dt.month
    col_year = col_tmsp.dt.year
    col_weekday = col_tmsp.dt.dayofweek
    # insert time data records
    column_labels = ("start_time", "day", "week", "month", "year", "weekday")
    time_df = pd.concat([col_start_time, col_day, col_week, col_month, col_year, col_weekday], axis=1,
                        ignore_index=True)
    time_df.columns = column_labels
    time_df.drop_duplicates(inplace=True)
    print(f'Length of time_df :{time_df.shape[0]}')
    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except Exception as e:
            print(e)


def load_user_table(cur, df_logs):
    """
        Reads user data from log dataframe and inserts into T_USER table
    """
    user_df = df_logs[['userId', 'firstName', 'lastName', 'gender', 'level']]
    # Remove records where first_name and last_name not present
    user_df = user_df[
        (~user_df['userId'].isnull()) & (~user_df['firstName'].isnull()) & (~user_df['lastName'].isnull())]
    user_df.reset_index(inplace=True, drop=True)
    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except Exception as e:
            print(e)


def process_data(cur, conn, filepath, func):
    """
        Get all filenames matching extension from directory
    """
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
        print(datafile)
        func(cur, datafile)
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """

    - Establishes connection with the sparkify database and gets cursor to it.

    - Read data from song and log files

    - Inserts data in different tables

    - Finally, closes the connection.

    """

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    conn.set_session(autocommit=True)
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
