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
    conn = psycopg2.connect("host={redshift-cluster-1.ca9ny7ohyv8p.us-east-1.redshift.amazonaws.com:5439/dev} dbname={dev} user={awsuser} password={A251d851!} port={5439}".format(*config['CLUSTER'].values())) #host - cluster endpoint
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()