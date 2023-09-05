import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

<p>Load staging tables from an S3 storage to a Redshift cluster.

The function takes arguments `cur` (cursor object) and `conn` (psycopg2 connection object).
It executes each query listed in the global variable `copy_table_queries` and commits
(saves changes in the database) each time.</p>

def load_staging_tables(cur, conn):
for query in copy_table_queries:
cur.execute(query)
conn.commit()

<p>Insert data from loaded staging tables into required tables.

The function takes arguments `cur` (cursor object) and `conn` (psycopg2 connection object).
It executes each query in the global variable `insert_table_queries` and commits
(saves changes in the database) each time.</p>

def insert_tables(cur, conn):
for query in insert_table_queries:
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