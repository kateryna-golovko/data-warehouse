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

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
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
