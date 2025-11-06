import os
import snowflake.connector
from pathlib import Path

print("🚀 Starting Snowflake deployment...")

# ✅ Fetch Snowflake credentials and stage name from environment
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")
stage = os.getenv("SNOWFLAKE_STAGE")  # Dynamic stage name

# ✅ Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=database,
    schema=schema
)
cursor = conn.cursor()

# ✅ Set session context explicitly
print(f"🔧 Setting context: DATABASE={database}, SCHEMA={schema}")
cursor.execute(f"USE DATABASE {database}")
cursor.execute(f"USE SCHEMA {schema}")

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
                            sql = f.read().strip()
                        cursor.execute(f"USE DATABASE {database}")
                        cursor.execute(f"USE SCHEMA {schema}")
                        cursor.execute(sql)
                        print(f"✅ Deployed: {sql_file}")
                        # ✅ Refresh pipe after creation
                        if folder == "pipes":
                            pipe_name = sql_file.stem
                            refresh_query = f"ALTER PIPE {database}.{schema}.{pipe_name} REFRESH"
                            try:
                                cursor.execute(refresh_query)
                                print(f"🔄 Pipe refreshed: {pipe_name}")
                            except Exception as e:
                                print(f"⚠️ Pipe refresh failed: {pipe_name} - {e}")
                    except Exception as e:
                        print(f"❌ Deployment failed: {sql_file} - {e}")

# ✅ Upload CSV files to stage and ingest manually
data_path = Path("data")
if data_path.exists():
    for csv_file in data_path.glob("*.csv"):
        stage_name = f"{stage}"  # Use stage name as defined in Snowflake
        put_command = f"PUT file://{csv_file.resolve()} @{stage_name} AUTO_COMPRESS = FALSE"
        print(f"📤 Uploading: {csv_file.name} to @{stage_name}")
        try:
            cursor.execute(put_command)
            print(f"✅ Uploaded: {csv_file.name}")

            # ✅ Manual ingestion using COPY INTO
            copy_query = f"""
            COPY INTO {database}.{schema}.sample_sales
            FROM @{stage_name}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
            """
            print(f"🔄 Running manual COPY INTO for {csv_file.name}...")
            cursor.execute(copy_query)
            print(f"✅ Data ingested into sample_sales table from {csv_file.name}")

        except Exception as e:
            print(f"❌ Upload or ingestion failed: {csv_file.name} - {e}")
else:
    print("📂 No data/ folder found. Skipping CSV upload.")

cursor.close()
conn.close()
print("✅ Deployment complete!")