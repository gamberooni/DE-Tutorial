import configparser
import psycopg2 as pg2
from sql_queries import *

def load_staging_tables(cur, conn):
    """
    load json data (log_data and song_data) from S3 and 
    insert into staging_events and staging_songs tables
    """
    for query in copy_table_queries:
        cur.execute(query)
    
def insert_tables(cur, conn):
    """
    insert data from staging tables (staging_events and staging_songs)
    into star schema analytics tables
    (fact table -- songplays
     dimension tables -- users, songs, artists, time)    
    """
    for query in insert_table_queries:
        cur.execute(query)
   
def main():
    # read config file 
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to redshift postgresql db
    conn = pg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # load data from json files to staging tables
    # then insert data into analysis tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()       
    
if __name__ == "__main__":
    main()