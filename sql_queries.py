import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
#This is step 3 - drop tables to avoid errors when rerruning the code
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# First step - create all the below tables (keep them empty), use the columns provided in the instructions
# The staging tables will have the same column structure as final columns 
staging_events_table_create= ("""
    CREATE TABLE staging_events (
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
    
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id int PRIMARY KEY, 
        start_time timestamp NOT NULL, 
        user_id int NOT NULL, 
        level varchar(50) NOT NULL, 
        song_id varchar(100) NOT NULL, 
        artist_id varchar(100) NOT NULL, 
        session_id int NOT NULL, 
        location varchar(255) NOT NULL, 
        user_agent varchar(500) NOT NULL   
    );
    
""")

user_table_create = ("""
    CREATE TABLE user (
    
    );
""")

song_table_create = ("""
    CREATE TABLE song (
    
    );
""")

artist_table_create = ("""
    CREATE TABLE artist (
    
    );
""")

time_table_create = ("""
    CREATE TABLE time (
    
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
