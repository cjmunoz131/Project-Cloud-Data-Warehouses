import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries



def load_staging_tables(cur, conn):
    """
        Description:
          Load input data (log_data and song_data in JSON format) from S3
          into staging_events and staging_songs tables.

        Function Arguments:
          cur --    cursor to execute the queries.
          conn --   connection reference (host, dbname, user, password, port)
                    used to enable the commit after the queries execution

        Output:
          song_data inserted in staging_songs table.
          log_data inserted in staging_events table.
          
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description:
      Insert data from staging tables into the analytics tables (star model)
      Analytics Tables:
        Fact Table: songplays
        Dimension Tables:
         - users
         - songs
         - artists
         - time
    Function Arguments:
      cur --    cursor to execute the queries.
      conn --   connection reference (host, dbname, user, password, port)
                used to enable the commit after the queries execution

    Output:
      data inserted and tranformed into songplays table.
      data inserted  into users table.
      data inserted  into songs table.
      data inserted  into artists table.
      data inserted and tranformed into time table.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description:
      main function
      - Read the cfg file, load the environment variables
      - Establish connection to redshift database
      - Load Staging tables
      - Insert data into analytics tables
    Function Arguments:
      None

    Output:
      All data processed and loaded into tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print('Loading job completed successfully')
    conn.close()


if __name__ == "__main__":
    main()