import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This query will be responsible for extracting data from aws s3 to staging tables which includes staging_events table and staging_songs

    Parameters:
    cur: database connection cursor
    conn: database connection

    Returns:
    None
   """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    This query will be responsible for transforming data from staging tables to dimension tables

    Parameters:
    cur: database connection cursor
    conn: database connection

    Returns:
    None
   """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()