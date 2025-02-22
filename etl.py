import configparser # Read configuration file
import psycopg2 # PostgreSQL adopter to interact with Redshift
from sql_queries import copy_table_queries, insert_table_queries # Import copy and insert queries for later execution

def load_staging_tables(cur, conn, config):
    """
    The function to load data from the S3 bucket into the earlier created staging tables:
        - Added the config input during debugging 
        - For each copy query, replace the variable names with the actual values from the config file
        - Execute the above query to load the data into the staging tables
        - Commit the transaction to Redshift
    """
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
    """
    The function to insert the data from the staging tables into the final tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    The main function that runs the ETL pipeline:
        - Read the config file that contains the actual values for the Redshift cluster, IAM role, etc.
        - Extract values from the config file
        - Establish connection to Redshift,
        - Create a cursor to interact with the database
        - Call the functions created above:
            - Load the data from S3 into the staging tables
            - Insert the data from the staging tables into the final tables
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
    
    load_staging_tables(cur, conn, config) 
    insert_tables(cur, conn) 
    
    conn.close()

# When the script is being executed (e.g. using python etl.py), run the main function
if __name__ == "__main__":
    main()