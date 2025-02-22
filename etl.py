import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn, config):
    # Update the queries with the config values before executing
    for query in copy_table_queries:
        query = query.format(
            log_data=config['S3']['LOG_DATA'], 
            arn=config['IAM_ROLE']['ARN'], 
            log_jsonpath=config['S3']['LOG_JSONPATH'],
            song_data=config['S3']['SONG_DATA']
        )
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
    
    #load_staging_tables(cur, conn, config)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()