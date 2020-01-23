import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

"""
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
 """
def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id','year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)

"""
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the song's timestamp information in order to store it along with realted hour, day, month etc into the time table.
    Then it extracts the users information in order to store it into the users table.
    Finally, it extracts the song_id and artist_id from songs table and insert them into songsplay table along with othere attributes like user_id, level, session_id etc.
    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
 """
def process_log_file(cur, filepath):
    # open log file
    df =  pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t =pd.to_datetime(df["ts"])

    print(t.to_frame())
    # insert time data records
    time_data = [df["ts"], t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ["timestamp","hour", "day", "weekofyear", "month", "year", "weekday"]
    dft=pd.DataFrame(time_data).T
    dft.columns = column_labels
    time_df = dft

    for i, row in time_df.iterrows():
        #print("row is:", row)
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

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
        songplay_data = (row.ts, row.userId,row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

"""
    This function walks through all the path, find all json files and pass the cursor, connection and each json path to the passed data process function

    INPUTS: 
    * cur the cursor variable
    * conn the database connection
    * filepath the file path for finding all json files
    * func this will be the function to called to process each json file
 """
def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()