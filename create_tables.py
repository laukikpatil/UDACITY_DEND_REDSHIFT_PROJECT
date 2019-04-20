import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
        """
        Drop tables from redshift database using the query array defined in sql_queries.py.

        Parameters:
        cur: Database connection cursor 
        conn: Database connection
        dwh_role_arn: AWS IAM role used to read files from S3 bucket
        region: region for the S3 bucket
        """

    # Open for loop and run the drop table queries.
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
        """
        Create tables in redshift database using the query array defined in sql_queries.py.

        Parameters:
        cur: Database connection cursor 
        conn: Database connection
        dwh_role_arn: AWS IAM role used to read files from S3 bucket
        region: region for the S3 bucket
        """

    # Open for loop and run the create table queries.
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get("DWH","DWH_ENDPOINT"), config.get("DWH","DWH_DB"), \
            config.get("DWH","DWH_DB_USER"), config.get("DWH","DWH_DB_PASSWORD"), config.get("DWH","DWH_PORT")))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()