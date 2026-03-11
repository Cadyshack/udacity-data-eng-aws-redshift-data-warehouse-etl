import psycopg
from redshift_etl.scripts.config_helper import get_config
from redshift_etl.sql.sql_queries import copy_table_queries, insert_table_queries
from redshift_etl.sql.data_quality import data_quality_checks


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def run_quality_checks(cur):
    print("\n--- Running Data Quality Checks ---\n")
    failed = 0
    passed = 0

    for check in data_quality_checks:
        cur.execute(check["check_sql"])
        result = cur.fetchone()

        if check["expected"](result):
            print(f"PASSED: {check['description']} (result: {result[0]})")
            passed += 1
        else:
            print(f"FAILED: {check['description']} (result: {result[0]})")
            failed += 1

    print(f"\n--- Quality Checks Complete: {passed} passed, {failed} failed ---")

    if failed > 0:
        raise ValueError(f"Data quality checks failed: {failed} check(s) did not pass.")
    

def main():
    config = get_config()
    HOST = config.get('CLUSTER', 'HOST')
    DB_NAME = config.get('CLUSTER', 'DB_NAME')
    DB_USER = config.get('CLUSTER', 'DB_USER')
    DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
    DB_PORT = config.get('CLUSTER', 'DB_PORT')

    conn = psycopg.connect("host={} dbname={} user={} password={} port={}".format(HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    run_quality_checks(cur)

    conn.close()


if __name__ == "__main__":
    main()