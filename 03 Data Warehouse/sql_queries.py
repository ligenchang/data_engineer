import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events 
(
artist VARCHAR(255),
auth VARCHAR(255),
first_name VARCHAR(255),
gender VARCHAR(255),
item_in_session INTEGER,
last_name VARCHAR(255),
length FLOAT,
user_level VARCHAR(255),
location VARCHAR(255),
method VARCHAR(255),
page VARCHAR(255),
registration VARCHAR(255),
session_id INTEGER,
song_title VARCHAR(255),
status INTEGER,
TIMESTAMP VARCHAR(255),
user_agent VARCHAR(255),
user_id int
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs
(
song_id VARCHAR(255),
title VARCHAR(255),
duration FLOAT,
artist_id VARCHAR(255),
artist_name VARCHAR(255),
artist_location VARCHAR(255),
artist_latitude FLOAT,
artist_longitude FLOAT,
year INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE songplays 
(
songplay_id INTEGER IDENTITY(0,1),
start_time VARCHAR(255) NOT NULL,
user_id VARCHAR(255) NOT NULL,
level VARCHAR(255),
song_id VARCHAR(255) NOT NULL,
artist_id VARCHAR(255) NOT NULL,
session_id VARCHAR(255),
location VARCHAR(255),
user_agent VARCHAR(255),
PRIMARY KEY (songplay_id)
)
""")

user_table_create = ("""
CREATE TABLE users
(
user_id INTEGER,
first_name VARCHAR(255),
last_name VARCHAR(255),
gender VARCHAR(255),
level VARCHAR(255),
PRIMARY KEY (user_id)
)
""")

song_table_create = ("""
CREATE TABLE songs 
(
song_id VARCHAR(255),
title VARCHAR(255),
artist_id VARCHAR(255) NOT NULL,
year INTEGER,
duration FLOAT,
PRIMARY KEY (song_id)
)
""")

artist_table_create = ("""
CREATE TABLE artists 
(
artist_id VARCHAR(255),
name VARCHAR(255),
location VARCHAR(255),
latitude FLOAT,
longitude FLOAT,
PRIMARY KEY (artist_id)
)
""")

time_table_create = ("""
CREATE TABLE time 
(
start_time VARCHAR(255),
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday INTEGER,
PRIMARY KEY (start_time)
)
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from '{}'
 credentials 'aws_iam_role={}' 
 json '{}'
 """).format(config.get('S3','LOG_DATA'), 
             config.get('IAM_ROLE', 'ARN'), 
             config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs from '{}' 
credentials 'aws_iam_role={}' 
json 'auto';
""").format(config["S3"]["SONG_DATA"], 
            config["IAM_ROLE"]["ARN"])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT DISTINCT 
    TIMESTAMP 'epoch' + TIMESTAMP/1000 *INTERVAL '1 second' as start_time, 
    event.user_id, 
    event.user_level,
    song.song_id,
    song.artist_id,
    event.session_id,
    event.location,
    event.user_agent
FROM staging_events event, staging_songs song
WHERE event.song_title = song.title and event.page = 'NextSong' 
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)  
SELECT DISTINCT 
    user_id,
    first_name,
    last_name,
    gender, 
    user_level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT 
    song_id, 
    title,
    artist_id,
    year,
    duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT 
    start_time, 
    EXTRACT(hr from start_time) AS hour,
    EXTRACT(d from start_time) AS day,
    EXTRACT(w from start_time) AS week,
    EXTRACT(mon from start_time) AS month,
    EXTRACT(yr from start_time) AS year, 
    EXTRACT(weekday from start_time) AS weekday 
FROM (
    SELECT DISTINCT  TIMESTAMP 'epoch' + TIMESTAMP/1000 *INTERVAL '1 second' as start_time 
    FROM staging_events     
    )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
