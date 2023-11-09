import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
    Description:
      Drop all the tables: staging tables and analytics tables

    Function Arguments:
      cur --    cursor to execute the queries.
      conn --   connection reference (host, dbname, user, password, port)
                used to enable the commit after the queries execution

    Output:
      Drop all the tables
      
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

"""
    Description:
      Create all the tables: staging tables and analytics tables (Star schema model: songplays, users, artists, songs, time)

    Function Arguments:
      cur --    cursor to execute the queries.
      conn --   connection reference (host, dbname, user, password, port)
                used to enable the commit after the queries execution

    Output:
      Create all the tables (staging_events, staging_songs, songplays, users, artists, songs, time)
      
"""
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


"""
    Description:
      main function
      - Read the cfg file, load the environment variables
      - Establish connection to redshift database
      - Drop all the tables
      - Create all the tables
    Function Arguments:
      None

    Output:
      All the tables are created, staging tables and analytics tables (Star model)
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