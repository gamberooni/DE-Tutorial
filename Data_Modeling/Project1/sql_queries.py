# CREATE database
create_database = "CREATE DATABASE sparkifydb"

# CREATE tables
create_songplays = \
    "CREATE TABLE IF NOT EXISTS songplays( \
    songplay_id SERIAL PRIMARY KEY, \
    start_time TIMESTAMP, \
    user_id INT, \
    level VARCHAR(20), \
    song_id VARCHAR(50), \
    artist_id VARCHAR(50), \
    session_id INT, \
    location varchar(200), \
    user_agent varchar(200))"
    
create_users = \
    "CREATE TABLE IF NOT EXISTS users( \
    user_id INT UNIQUE PRIMARY KEY, \
    first_name VARCHAR(200) NOT NULL, \
    last_name VARCHAR(200) NOT NULL, \
    gender CHAR, \
    level VARCHAR(20))"
 
create_songs = \
    "CREATE TABLE IF NOT EXISTS songs( \
    song_id VARCHAR(200) UNIQUE PRIMARY KEY NOT NULL, \
    title VARCHAR(200) NOT NULL, \
    artist_id VARCHAR(50) NOT NULL, \
    year INT, \
    duration FLOAT(3))"    
    
create_artists = \
    "CREATE TABLE IF NOT EXISTS artists( \
    artist_id VARCHAR(50) PRIMARY KEY NOT NULL, \
    name VARCHAR(200) NOT NULL, \
    location VARCHAR(50), \
    latitude FLOAT(4), \
    longitude FLOAT(4))"
    
create_time = \
    "CREATE TABLE IF NOT EXISTS time( \
    start_time TIMESTAMP PRIMARY KEY, \
    hour INT, \
    day INT, \
    week INT,\
    month INT, \
    year INT, \
    weekday INT)"

# INSERT INTO tables
songplay_table_insert = \
    "INSERT INTO songplays(songplay_id, start_time, user_id, level, song_id, \
    artist_id, session_id, location, user_agent) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) \
    ON CONFLICT(songplay_id) DO NOTHING;"

user_table_insert = \
    "INSERT INTO users(user_id, first_name, last_name, gender, level) \
    VALUES (%s, %s, %s, %s, %s) \
    ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;"


song_table_insert = \
    "INSERT INTO songs(song_id, title, artist_id, year, duration) \
    VALUES (%s, %s, %s, %s, %s) \
    ON CONFLICT DO NOTHING;"

artist_table_insert = \
    "INSERT INTO artists(artist_id, name, location, latitude, longitude) \
    VALUES (%s, %s, %s, %s, %s) \
    ON CONFLICT DO NOTHING;"


time_table_insert = \
    "INSERT INTO time(start_time, hour, day, week, month, year, weekday) \
    VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;"
    
# DROP TABLES
songplays_table_drop = "DROP TABLE IF EXISTS songplays"
users_table_drop = "DROP TABLE IF EXISTS users"
songs_table_drop = "DROP TABLE IF EXISTS songs"
artists_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# FIND SONGS
song_select = \
    "SELECT ss.song_id, ss.artist_id FROM songs ss \
    JOIN artists ars on ss.artist_id = ars.artist_id \
    WHERE ss.title = %s \
    AND ars.name = %s \
    AND ss.duration = %s;"