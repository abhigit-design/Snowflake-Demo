import os
import snowflake.connector
from pathlib import Path

print("🔍 Starting DDL extraction...")

# Debug: Show environment variables (except password)
print(f"User: {os.getenv('SNOWFLAKE_USER')}")
print(f"Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
print(f"Database: {os.getenv('SNOWFLAKE_DATABASE')}")
print(f"Warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE')}")

# Connect to Snowflake
try:
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE")
    )
    cursor = conn.cursor()
    print("✅ Connected to Snowflake")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit(1)

base_path = Path("snowflake-pipelines/client1")
object_type_map = {
    "tables": "table",
    "file_formats": "file_format",
    "pipes": "pipe",
    "streams": "stream",
    "tasks": "task"
    # Stages handled separately
}

updated_files = 0
schema_name = os.getenv("SNOWFLAKE_SCHEMA")

# ✅ Create folders dynamically
for folder_name in ["tables", "stages", "file_formats", "streams", "pipes", "tasks"]:
    (base_path / folder_name / schema_name).mkdir(parents=True, exist_ok=True)

# ✅ Extract DDL for supported object types
for folder_name, snowflake_type in object_type_map.items():
    print(f"🔍 Extracting {snowflake_type}s...")
    cursor.execute(f"SHOW {snowflake_type}s IN SCHEMA {schema_name}")
    objects = cursor.fetchall()

    for obj in objects:
        object_name = obj[1]  # Name from SHOW command
        query = f"SELECT GET_DDL('{snowflake_type}', '{schema_name}.{object_name}')"
        print(f"➡ Running: {query}")
        try:
            cursor.execute(query)
            ddl = cursor.fetchone()[0]

            # Write to file
            sql_file = base_path / folder_name / schema_name / f"{object_name}.sql"
            with open(sql_file, "w") as f:
                f.write(ddl)
            updated_files += 1
            print(f"✅ Updated {sql_file}")
        except Exception as e:
            print(f"❌ Failed for {schema_name}.{object_name}: {e}")

# ✅ Handle Stages separately
print("🔍 Extracting stages...")
cursor.execute(f"SHOW STAGES IN SCHEMA {schema_name}")
stages = cursor.fetchall()

for stage in stages:
    stage_name = stage[1]
    url = stage[6] or ''
    storage_integration = stage[7] or ''
    comment = stage[8] or ''

    ddl = f"CREATE OR REPLACE STAGE {stage_name}"
    if url:
        ddl += f" URL = '{url}'"
    if storage_integration:
        ddl += f" STORAGE_INTEGRATION = {storage_integration}"
    if comment:
        ddl += f" COMMENT = '{comment}'"
    ddl += ";"

    sql_file = base_path / "stages" / schema_name / f"{stage_name}.sql"
    with open(sql_file, "w") as f:
        f.write(ddl)
    updated_files += 1
    print(f"✅ Updated {sql_file}")

cursor.close()
conn.close()

if updated_files == 0:
    print("❌ No files updated. Check database/schema or credentials.")
    exit(1)

print(f"🎉 Extraction complete! Updated {updated_files} files.")
