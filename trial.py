import os
import snowflake.connector
conn = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse='dagster_wh',
    database='dagster_db',
    schema='mflix',
    role='dagster_role'
)
cur = conn.cursor()
cur.execute("SHOW TABLES LIKE '%';")
print(cur.fetchall())
cur.close(); conn.close()