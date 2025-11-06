import os
import snowflake.connector
from pathlib import Path

print("🚀 Starting Snowflake deployment...")

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)
cursor = conn.cursor()

base_path = Path("snowflake-pipelines/client1")
folders = ["tables", "stages", "file_formats", "streams", "pipes", "tasks"]

# ✅ Deploy objects in correct order
for folder in folders:
    folder_path = base_path / folder
    if folder_path.exists():
        for schema_folder in folder_path.iterdir():
            if schema_folder.is_dir():
                for sql_file in schema_folder.glob("*.sql"):
                    print(f"➡ Deploying: {sql_file}")
                    try:
                        with open(sql_file, "r") as f:
                            sql = f.read()
                        cursor.execute(sql)
                        print(f"✅ Deployed: {sql_file}")
                        # ✅ Refresh pipe after creation
                        if folder == "pipes":
                            pipe_name = sql_file.stem
                            schema_name = schema_folder.name
                            refresh_query = f"ALTER PIPE {schema_name}.{pipe_name} REFRESH"
                            try:
                                cursor.execute(refresh_query)
                                print(f"🔄 Pipe refreshed: {schema_name}.{pipe_name}")
                            except Exception as e:
                                print(f"⚠️ Pipe refresh failed: {schema_name}.{pipe_name} - {e}")
                    except Exception as e:
                        print(f"❌ Deployment failed: {sql_file} - {e}")

# ✅ Upload CSV files to stage
data_path = Path("data")
if data_path.exists():
    for csv_file in data_path.glob("*.csv"):
        stage_name = "PUBLIC.MY_STAGE"  # Replace with your actual stage name
        put_command = f"PUT file://{csv_file.resolve()} @{stage_name}"
        print(f"📤 Uploading: {csv_file.name} to @{stage_name}")
        try:
            cursor.execute(put_command)
            print(f"✅ Uploaded: {csv_file.name}")
        except Exception as e:
            print(f"❌ Upload failed: {csv_file.name} - {e}")
else:
    print("📂 No data/ folder found. Skipping CSV upload.")

cursor.close()
conn.close()
print("✅ Deployment complete!")