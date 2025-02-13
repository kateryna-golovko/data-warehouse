import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

   # Extract values from the config file
    host = config['CLUSTER']['HOST'] 
    dbname = config['CLUSTER']['DB_NAME']
    user = config['CLUSTER']['DB_USER']
    password = config['CLUSTER']['DB_PASSWORD']
    port = config['CLUSTER']['DB_PORT']
    iam_role_arn = config['IAM_ROLE']['ARN']
    staging_logdata_bucket = config['S3']['LOG_DATA']
    staging_jsonpath_bucket = config['S3']['LOG_JSONPATH']
    staging_songs_bucket = config['S3']['SONG_DATA']
    
    # Establish connection to Redshift
    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port
    )
    
    # Create a cursor to interact with the database
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()