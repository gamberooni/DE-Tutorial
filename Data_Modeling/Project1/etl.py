import os
import glob
import psycopg2 as pg2
import pandas as pd
import numpy as np
from sql_queries import *

# get all the json files
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))    
    return all_files

# process the song files    
def process_song_files():
    song_files = get_files("data/song_data")
    for filepath in song_files:
        df = pd.read_json(filepath, lines=True) 
        # print(df.head())
        song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
        cur.execute(song_table_insert, song_data)
        
        # get artist data and insert into artists table
        artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
        cur.execute(artist_table_insert, artist_data)    

# process the log files
def process_log_files():
    log_files = get_files("data/log_data")
    for filepath in log_files:
        df = pd.read_json(filepath, lines=True)
        df = df.query("page == 'NextSong'")
        # extract and transform the timestamp into desired format
        # insert into time table
        t = pd.to_datetime(df['ts'], unit='ms')
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        time_data = list((t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday))
        column_labels = list(('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'))
        time_df =  pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))
        
        # extract user data and insert into users table
        user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
        
        # insert into songplays tables
        for index, row in df.iterrows():    
            # perform sql filtering to get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
            
            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId,\
                             row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)

# define parameters required to connect to PostgreSQL
hostname = 'localhost'
username = 'postgres'
password = 'password'
database = 'sparkifydb' # specify db name if connecting to one
conn = pg2.connect(host=hostname, user=username, password=password, database=database)

cur = conn.cursor() # create a cursor object
conn.set_session(autocommit=True)

process_song_files()
# must process log files after processing song files 
# because inserting into songplays table requires info from song_files table
process_log_files() 

cur.close()
conn.close()
