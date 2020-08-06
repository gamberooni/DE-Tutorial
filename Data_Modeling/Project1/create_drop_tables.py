import psycopg2 as pg2
from sql_queries import *

# define parameters required to connect to PostgreSQL
hostname = 'localhost'
username = 'postgres'
password = 'password'
database = 'sparkifydb' # specify db name if connecting to one
conn = pg2.connect(host=hostname, user=username, password=password, database=database)

cur = conn.cursor() # create a cursor object
conn.set_session(autocommit=True) # to avoid manually commit every time a transaction is executed 
    
# create database
# cur.execute(create_database)

# create tables
cur.execute(create_songplays)
cur.execute(create_users)    
cur.execute(create_songs)
cur.execute(create_artists)
cur.execute(create_time)

# drop tables
# cur.execute(songplays_table_drop)
# cur.execute(users_table_drop)
# cur.execute(songs_table_drop)
# cur.execute(artists_table_drop)
# cur.execute(time_table_drop)

cur.close()
conn.close()