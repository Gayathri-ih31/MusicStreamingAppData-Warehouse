import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

<p>Drop tables if exists in Redshift Cluster

The function takes arguments `cur` (cursor object) and `conn` (psycopg2 connection object).
It executes each query listed in the global variable `drop_table_queries` and commits
(saves changes in the database) each time.</p>

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

<p>Create tables if not exist in Redshift Cluster

The function takes arguments `cur` (cursor object) and `conn` (psycopg2 connection object).
It executes each query listed in the global variable `create_table_queries` and commits
(saves changes in the database) each time.</p>

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


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