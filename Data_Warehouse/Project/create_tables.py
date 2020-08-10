import psycopg2 as pg2
import configparser
from sql_queries import *


def drop_tables(cur, conn):
    # drops tables -- songplays, users, artists, songs, time
    for query in drop_table_queries:
        cur.execute(query)
        
def create_tables(cur, conn):
    # creates tables -- songplays, users, artists, songs, time
    for query in create_table_queries:
        cur.execute(query)

def main():
    # read the config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')    
    
    conn = pg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    
if __name__ == "__main__":
    main()