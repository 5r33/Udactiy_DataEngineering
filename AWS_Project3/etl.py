import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - Load each stage table using the queries in `copy_table_queries` list.
    - Copies data from S3 into Redshift database and can be considered as
        intermediate storage for the raw data.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - Insert data into the fact and Dim table in `insert_table_queries` list.
    - source for the data is the intermediate staging table created.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """   
    - Establishes connection with the sparkify database and gets
    cursor to it.      
    - Copies data into the Staging table.      
    - Inserts data from Staging to Fact and Dim tables.     
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    """
    - Creates and connects to the redshift DB
    - Returns the connection and cursor to sparkifydb
    """
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()