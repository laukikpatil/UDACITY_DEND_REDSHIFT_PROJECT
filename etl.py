import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn, dwh_role_arn, region):
        """
        Load data from S3 bucket for the copy query array defined in sql_queries.py.

        Parameters:
        cur: Database connection cursor 
        conn: Database connection
        dwh_role_arn: AWS IAM role used to read files from S3 bucket
        region: region for the S3 bucket
        """

    # Open for loop and run the copy queries to load data to staging tables.
    for query in copy_table_queries:
        cur.execute(query.format(dwh_role_arn,region ))
        conn.commit()


def insert_tables(cur, conn):
        """
        Load data from staging tables to dimension and fact tables using query array defined in sql_queries.py.

        Parameters:
        cur: Database connection cursor 
        conn: Database connection
        dwh_role_arn: AWS IAM role used to read files from S3 bucket
        region: region for the S3 bucket
        """

    # Open for loop and run the insert commands to load data to dimension and fact tables.
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    REGION = config.get("AWS","REGION")
    dwh_role_arn = config.get("DWH","dwh_role_arn")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get("DWH","DWH_ENDPOINT"), config.get("DWH","DWH_DB"), \
            config.get("DWH","DWH_DB_USER"), config.get("DWH","DWH_DB_PASSWORD"), config.get("DWH","DWH_PORT")))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn, dwh_role_arn, REGION)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()