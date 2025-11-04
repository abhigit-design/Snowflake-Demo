import snowflake.connector
import os

# Connect to Snowflake using environment variables
conn = snowflake.connector.connect(
    user=os.getenv("SNOWSQL_USER"),
    password=os.getenv("SNOWSQL_PASSWORD"),
    account=os.getenv("SNOWSQL_ACCOUNT"),
    role=os.getenv("SNOWSQL_ROLE"),
    warehouse=os.getenv("SNOWSQL_WAREHOUSE"),
    database=os.getenv("SNOWSQL_DATABASE"),
    schema=os.getenv("SNOWSQL_SCHEMA")
)

# Create a cursor
cur = conn.cursor()

try:
    # Load SQL from file
    with open("scripts/load_data.sql", "r") as f:
        sql_script = f.read()

    # Split and execute each statement (if multiple)
    for statement in sql_script.strip().split(";"):
        if statement.strip():  # skip empty statements
            print(f"Executing: {statement.strip()}")
            cur.execute(statement)

    print("SQL script executed successfully.")

finally:
    cur.close()
    conn.close()