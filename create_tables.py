import configparser # Read configuration file
import psycopg2 # PostgreSQL adopter to interact with Redshift
from sql_queries import create_table_queries, drop_table_queries # Import queries for tables drop and creation

# The function that drops the existing tables to avoid errors if re-run of the script is required
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# The function that creates empty staging and final tables with defined column names and types
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# The main function that runs the table creation process
def main():
    # Read the config file that contains the actual values for the Redshift cluster, IAM role, etc.
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Extract values from the config file
    host = config['CLUSTER']['HOST'] # cluster is cluster`s endpoint, but without the ':' and stuff after it, e.g. no ':5439/dev'
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

    # Call the functions created above 
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Close the connection
    conn.close()

# When the script is being executed (e.g. using python create_tables.py), run the main function
if __name__ == "__main__":
    main()