import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        This function connects to the Warehouse database and loads the staging tables
        :param cur: cursor object
        :param conn: connection object
    """
    counter = 1
    for query in copy_table_queries:
        print(f"Starting query execution for: {query}")
        cur.execute(query)
        conn.commit()
        print(f"Copy query finished!")


def insert_tables(cur, conn):
    """
        This function connects to the Warehouse database and bulk inserts into the various 
        analytics tables
        :param cur: cursor object
        :param conn: connection object
    """
    for query in insert_table_queries:
        print(f"Starting query execution for: {query}")
        cur.execute(query)
        conn.commit()
        print(f"Insert query finished!")


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