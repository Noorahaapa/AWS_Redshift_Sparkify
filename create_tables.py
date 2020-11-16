import configparser
import psycopg2

from sql_queries import create_table_queries, drop_table_queries

    """
    This function drops tables using SQL queries defined in sql_queries.py
      
    """
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    """
    Here we create staging and final tables in Redshift using SQL queries defined in sql_queries.py
      
    """
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    """
    This function connects to database and executes the table creation using the two functions above
      
    """
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
    

    
    
