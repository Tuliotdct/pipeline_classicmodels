from src.connections.spark_connections import create_spark_session, spark_jdbc_connection
from dotenv import load_dotenv
import os

def create_bronze_for_table(table_name, execution_date=None):

    load_dotenv()

    bucket = os.getenv("S3_BUCKET_NAME")

    spark = create_spark_session()

    jdbc_url, props = spark_jdbc_connection()

    read_db_tables = spark.read.jdbc(url=jdbc_url, table=f'{table_name}', properties=props)

    count = read_db_tables.count()

    read_db_tables.write.mode("overwrite").parquet(f"s3a://{bucket}/bronze/{table_name}/{execution_date}")

    spark.stop()
    
    return {"table": table_name, "records": count, "status": "success"}

