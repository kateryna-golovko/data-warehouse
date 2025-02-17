import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
#This is step 3 - drop tables to avoid errors when rerruning the code
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user_info"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# First step - create all the below tables (keep them empty), use the columns provided in the instructions
# The staging tables will have the same column structure as final columns 
staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist varchar(100),
        auth varchar(255),
        firstName varchar(100),
        gender varchar(50),
        iteminSession int,
        lastName varchar(100),
        length numeric,
        level varchar(50),
        location varchar(255),
        method varchar(50),
        page varchar(50),
        registration numeric,
        sessionId int,
        song varchar(255),
        status int,
        ts int,
        userAgent varchar(500),
        userId int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs int,
        artist_id varchar(100),
        artist_latitude varchar(100),
        artist_longitude varchar(100),
        artist_location varchar(255),
        artist_name varchar(100),
        song_id varchar(100),
        title varchar(255),
        duration float,
        year int 
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id int PRIMARY KEY, 
        start_time timestamp NOT NULL, 
        user_id int NOT NULL, 
        level varchar(50), 
        song_id varchar(100) NOT NULL, 
        artist_id varchar(100) NOT NULL, 
        session_id int NOT NULL, 
        location varchar(255), 
        user_agent varchar(500)    
    );
    
""")

user_table_create = ("""
    CREATE TABLE user_info (
        user_id int PRIMARY KEY,
        firstname varchar(100),
        lastname varchar(100),
        gender varchar(50),
        level varchar(50)
    );
""")

song_table_create = ("""
    CREATE TABLE song (
        song_id varchar(100) PRIMARY KEY,
        title varchar(255),
        artist_id varchar(100),
        year int,
        duration float
    );
""")

artist_table_create = ("""
    CREATE TABLE artist (
        artist_id varchar(100) PRIMARY KEY,
        name varchar(100),
        location varchar(255),
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

#specify that the file is JSON format
#add that the invalid characters will be replaced with '?' during loading to avoid failed load
staging_events_copy = (""" COPY staging_events FROM '{LOG_DATA}'
CREDENTIALS 'aws_iam_role={ARN}'
FORMAT AS JSON 'auto'
REGION 'us-west-2'
ACCEPTINVCHARS AS '?'
""").format(LOG_DATA, ARN)

staging_songs_copy = (""" COPY staging_songs FROM '{SONG_DATA}'
CREDENTIALS 'aws_iam_role={ARN}'
REGION 'us-west-2'
ACCEPTINVCHARS AS '?'
""").format(SONG_DATA, ARN)

# FINAL TABLES
# songplay_id - generate a unique id - use ROW_NUMBER() function to generate a unique integer for each row within a result set- The number is assigned based on the order defined by the ORDER BY clause - order by timestamp to ensure the correct order of the generated IDs
# start_time - converting timestamp from milliseconds to seconds
songplay_table_insert = (""" INSERT INTO songplay
(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT ROW_NUMBER() OVER (ORDER BY e.ts) as songplay_id, 
to_timestamp(e.ts/1000) as start_time, 
e.userId as user_id,
e.level as level,
s.song_id as song_id,
s.artist_id as artist_id,
e.sessionId as session_id,
s.artist_location as location,
e.userAgent as user_agent
FROM staging_songs s 
JOIN staging_events e on e.artist=s.artist_name
""")

user_table_insert = (""" INSERT INTO user_info
(user_id, firstname, lastname, gender, level)
SELECT DISTINCT userId as user_id,
firstName as firstname,
lastName as lastname,
gender,
level
FROM staging_events
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")
# After the logic for all the above tables is written, go to create_tables.py and fill in the connection to the DB
# This will create the above empty tables in Redshift

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
