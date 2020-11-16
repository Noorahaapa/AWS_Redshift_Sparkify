import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

    """
    This function loads songs and events log data to staging using queries in sql_queries.py
      
    """
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    
    """
    This function inserts data loaded to staging into the created tables
      
    """

def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

    """
    Here using the two functions above data is actually loaded to staging and into the created tables
      
    """
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