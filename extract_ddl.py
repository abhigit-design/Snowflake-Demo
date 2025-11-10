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
    "stages": "stage",
    "file_formats": "file_format",
    "pipes": "pipe",
    "streams": "stream",
    "tasks": "task"
}

updated_files = 0

for folder_name, snowflake_type in object_type_map.items():
    folder_path = base_path / folder_name
    if folder_path.exists():
        for schema_folder in folder_path.iterdir():
            if schema_folder.is_dir():
                schema_name = schema_folder.name
                for sql_file in schema_folder.glob("*.sql"):
                    object_name = sql_file.stem
                    query = f"SELECT GET_DDL('{snowflake_type}', '{schema_name}.{object_name}')"
                    print(f"➡ Running: {query}")
                    try:
                        cursor.execute(query)
                        ddl = cursor.fetchone()[0]
                        with open(sql_file, "w") as f:
                            f.write(ddl)
                        updated_files += 1
                        print(f"✅ Updated {sql_file}")
                    except Exception as e:
                        print(f"❌ Failed for {schema_name}.{object_name}: {e}")

cursor.close()
conn.close()

if updated_files == 0:
    print("❌ No files updated. Check database/schema or credentials.")
    exit(1)

print(f"🎉 Extraction complete! Updated {updated_files} files.")
