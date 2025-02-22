import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #Fill this in as a second step
    # Extract values from the config file
    host = config['CLUSTER']['HOST'] #cluster`s endpoint, but without the ':' and stuff after it, e.g. no ':5439/dev'
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
    
    # Debug if needed - if errors occur during connection, check whether everything is entered correctly
    #print(f"Host: {host}")
    #print(f"DB Name: {dbname}")
    #print(f"User: {user}")
    #print(f"Password: {password}")
    #print(f"Port: {port}")
    
    # Create a cursor to interact with the database
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()