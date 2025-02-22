import configparser # Read configuration file
import psycopg2 # PostgreSQL adopter to interact with Redshift
from sql_queries import create_table_queries, drop_table_queries # Import queries for tables drop and creation

def drop_tables(cur, conn):
    """
    The function that drops the existing tables to avoid errors if re-run of the script is required
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    The function that creates empty staging and final tables with defined column names and types
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    The main function that runs the table creation process:
        - Read the config file that contains the actual values for the Redshift cluster, IAM role, etc.
        - Extract values from the config file: cluster is cluster`s endpoint, but without the ':' and stuff after it, e.g. no ':5439/dev'
        - Establish connection to Redshift
        - Create a cursor to interact with the database
        - Call the functions created above 
        - Close the connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    host = config['CLUSTER']['HOST'] 
    dbname = config['CLUSTER']['DB_NAME']
    user = config['CLUSTER']['DB_USER']
    password = config['CLUSTER']['DB_PASSWORD']
    port = config['CLUSTER']['DB_PORT']
    
    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port
    )
    
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

# When the script is being executed (e.g. using python create_tables.py), run the main function
if __name__ == "__main__":
    main()