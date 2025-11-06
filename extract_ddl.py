import os
import snowflake.connector
from pathlib import Path

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE")
)
cursor = conn.cursor()

# Base path for your repo
base_path = Path("Snowflake-Demo/snowflake-pipelines/client1")

# Map folder names to Snowflake object types
object_type_map = {
    "tables": "table",
    "stages": "stage",
    "file_formats": "file_format",
    "pipes": "pipe",
    "streams": "stream",
    "tasks": "task"
}

# Iterate through folders and extract DDL
for folder_name, snowflake_type in object_type_map.items():
    folder_path = base_path / folder_name
    if folder_path.exists():
        for schema_folder in folder_path.iterdir():
            if schema_folder.is_dir():
                schema_name = schema_folder.name
                for sql_file in schema_folder.glob("*.sql"):
                    object_name = sql_file.stem
                    try:
                        query = f"SELECT GET_DDL('{snowflake_type}', '{schema_name}.{object_name}')"
                        cursor.execute(query)
                        ddl = cursor.fetchone()[0]
                        with open(sql_file, "w") as f:
                            f.write(ddl)
                        print(f"✅ Extracted DDL for {snowflake_type}: {schema_name}.{object_name}")
                    except Exception as e:
                        print(f"❌ Failed for {schema_name}.{object_name}: {e}")

cursor.close()
conn.close()
print("🎉 Extraction complete!")