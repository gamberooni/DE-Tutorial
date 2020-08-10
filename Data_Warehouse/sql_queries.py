import configparser
import boto3


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
LOG_DATA  = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

iam = boto3.client('iam',
                    region_name="ap-southeast-1",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET)

ARN = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES
# Staging tables
# redshift does not support SERIAL -- use IDENTITY(seed, step)
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
( 
    event_id BIGINT IDENTITY(0,1) NOT NULL,
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession VARCHAR,
    lastName VARCHAR,
    length VARCHAR,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INTEGER NOT NULL SORTKEY DISTKEY,
    song VARCHAR,
    status INTEGER,
    ts BIGINT NOT NULL,
    userAgent VARCHAR,
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(
    num_songs INTEGER,
    artist_id VARCHAR NOT NULL SORTKEY DISTKEY,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(500),
    artist_name VARCHAR(500),
    song_id VARCHAR NOT NULL,
    title VARCHAR(500),
    duration DECIMAL(9),
    year INTEGER
);
""")

# Analytics table
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays
(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY sortkey,
    start_time TIMESTAMP,
    user_id INTEGER,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users
(
    user_id INTEGER PRIMARY KEY distkey,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs
(
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR distkey,
    year INTEGER,
    duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists
(
    artist_id VARCHAR PRIMARY KEY distkey,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
(
    start_time TIMESTAMP PRIMARY KEY sortkey distkey,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
);
""")

# STAGING TABLES
# copy the log_data and song_data from another s3 bucket
staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO fact_songplays
(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
    
SELECT DISTINCT 
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    se.userId AS user_id,
    se.level AS level,
    ss.song_id AS song_id,
    ss.artist_id AS artist_id,
    se.sessionId AS session_id,
    se.location  AS location,
    se.userAgent AS user_agent
FROM staging_events AS se
JOIN staging_songs AS ss
ON (se.artist = ss.artist_name)
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO dim_users(
    user_id,
    first_name,
    last_name,
    gender,
    level)

SELECT  DISTINCT 
    se.userId AS user_id,
    se.firstName AS first_name,
    se.lastName AS last_name,
    se.gender AS gender,
    se.level AS level
FROM staging_events AS se
WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO dim_songs(                 
    song_id,
    title,
    artist_id,
    year,
    duration)

SELECT DISTINCT 
    ss.song_id AS song_id,
    ss.title AS title,
    ss.artist_id AS artist_id,
    ss.year AS year,
    ss.duration AS duration
FROM staging_songs AS ss;
""")

artist_table_insert = ("""
INSERT INTO dim_artists( 
    artist_id,
    name,
    location,
    latitude,
    longitude)
SELECT DISTINCT 
    ss.artist_id AS artist_id,
    ss.artist_name AS name,
    ss.artist_location AS location,
    ss.artist_latitude AS latitude,
    ss.artist_longitude AS longitude
FROM staging_songs AS ss;
""")

time_table_insert = ("""
INSERT INTO dim_time(                  
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(week FROM start_time) AS weekday
FROM staging_events AS se
WHERE se.page = 'NextSong';
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]