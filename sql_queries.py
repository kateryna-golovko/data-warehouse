import configparser # Read configuration file


# CONFIG
# Use the ConfigParser to read the configuration file dwh.cfg for database and AWS credentials
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
# Drop the existing tables to avoid errors when re-rruning the code
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user_info"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# Create emoty staging and final tables with column names and types defined
staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist varchar(255),
        auth varchar(255),
        firstName varchar(255),
        gender varchar(50),
        iteminSession bigint,
        lastName varchar(255),
        length numeric,
        level varchar(50),
        location varchar(500),
        method varchar(50),
        page varchar(100),
        registration numeric,
        sessionId bigint,
        song varchar(255),
        status int,
        ts bigint,
        userAgent varchar(500),
        userId bigint
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs int,
        artist_id varchar(255),
        artist_latitude varchar(255),
        artist_longitude varchar(255),
        artist_location varchar(500),
        artist_name varchar(255),
        song_id varchar(255),
        title varchar(255),
        duration float,
        year int 
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id int PRIMARY KEY, 
        start_time timestamp NOT NULL, 
        user_id bigint NOT NULL, 
        level varchar(100), 
        song_id varchar(255) NOT NULL, 
        artist_id varchar(255) NOT NULL, 
        session_id bigint NOT NULL, 
        location varchar(500), 
        user_agent varchar(500)    
    );
    
""")

user_table_create = ("""
    CREATE TABLE user_info (
        user_id bigint PRIMARY KEY,
        firstname varchar(255),
        lastname varchar(255),
        gender varchar(50),
        level varchar(50)
    );
""")

song_table_create = ("""
    CREATE TABLE song (
        song_id varchar(255) PRIMARY KEY,
        title varchar(255),
        artist_id varchar(255),
        year int,
        duration float
    );
""")

artist_table_create = ("""
    CREATE TABLE artist (
        artist_id varchar(255) PRIMARY KEY,
        name varchar(255),
        location varchar(500),
        latitude float,
        longitude float
    );
""")

time_table_create = ("""
    CREATE TABLE time (
        startime timestamp PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    );
""")

# STAGING TABLES
# Load the data from the S3 bucket into the staging tables, specify IAM role for S3 Read access for Redshift
# Specify that the file is JSON format
# The invalid characters will be replaced with '?' during loading to avoid failed load
staging_events_copy = ("""
    COPY staging_events FROM '{log_data}'
    CREDENTIALS 'aws_iam_role={arn}'
    FORMAT AS JSON '{log_jsonpath}'
    REGION 'us-west-2'
    ACCEPTINVCHARS AS '?' 
""")

staging_songs_copy = ("""
    COPY staging_songs FROM '{song_data}'
    CREDENTIALS 'aws_iam_role={arn}'
    FORMAT AS JSON 'auto'
    REGION 'us-west-2'
    ACCEPTINVCHARS AS '?' 
""")

# FINAL TABLES
# Load data from staging tables into the final tables within Redshift

# songplay_id - generate a unique id - use ROW_NUMBER() function to generate a unique integer for each row within a result set - The number is assigned based on the order defined by the ORDER BY clause - order by timestamp to ensure the correct order of the generated IDs
# start_time - converting timestamp from milliseconds to seconds
songplay_table_insert = (""" INSERT INTO songplay
(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT ROW_NUMBER() OVER (ORDER BY e.ts) as songplay_id, 
TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time, 
e.userId as user_id,
e.level as level,
s.song_id as song_id,
s.artist_id as artist_id,
e.sessionId as session_id,
s.artist_location as location,
e.userAgent as user_agent
FROM staging_songs s 
JOIN staging_events e on e.artist=s.artist_name
WHERE e.page='NextSong'
""")

user_table_insert = (""" INSERT INTO user_info
(user_id, firstname, lastname, gender, level)
SELECT DISTINCT userId as user_id,
firstName as firstname,
lastName as lastname,
gender,
level
FROM staging_events
WHERE page='NextSong'
""")

song_table_insert = (""" INSERT INTO song
(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
title,
artist_id,
year,
duration
FROM staging_songs
""")

artist_table_insert = (""" INSERT INTO artist
(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
artist_name as name,
artist_location as location,
CAST(artist_latitude AS DOUBLE PRECISION) AS latitude,
CAST(artist_longitude AS DOUBLE PRECISION) AS longitude
FROM staging_songs
""")

# Convert the timestamp from milliseconds into seconds and cast it to timestamp data type
# Extract hour, day, week, etc. from the timestamp
time_table_insert = (""" INSERT INTO time
(startime, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time,
EXTRACT(hour FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS hour,
EXTRACT(day FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS day,
EXTRACT(week FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS week,
EXTRACT(month FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS month,
EXTRACT(year FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS year,
EXTRACT(dow FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS weekday
FROM staging_events 
WHERE page='NextSong'
""")

# QUERY LISTS
# List of queries to create, drop, copy, and insert data into respective tables
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
