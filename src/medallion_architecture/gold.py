import boto3
from pyspark.sql import SparkSession
from src.connections.spark_connections import create_spark_session
from src.config.gold.gold_config import GOLD_TABLE_CONFIG
from dotenv import load_dotenv
import os

load_dotenv()

bucket = os.getenv("S3_BUCKET_NAME")
s3 = boto3.client('s3')
glue = boto3.client('glue')


def _gold_table_exists(spark, table_name):
    try:
        return spark.catalog.tableExists(f"glue_catalog.gold.{table_name}")
    except Exception as e:
        if "NotFoundException" in str(e) or "does not exist" in str(e).lower():
            try:
                glue.delete_table(DatabaseName="gold", Name=table_name)
            except glue.exceptions.EntityNotFoundException:
                pass
            return False
        raise


def create_gold_table(table_name):
    config = GOLD_TABLE_CONFIG[table_name]
    query = config["query"]

    spark = create_spark_session()
    try:
        # Create gold database if it doesn't exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.gold LOCATION 's3a://{bucket}/gold'")

        gold_table = f"glue_catalog.gold.{table_name}"

        # Drop table if exists to recreate it
        if _gold_table_exists(spark, table_name):
            spark.sql(f"DROP TABLE {gold_table}")

        # Execute the query and create the table
        df = spark.sql(query)
        df.writeTo(gold_table) \
            .tableProperty("write.format.default", "parquet") \
            .create()

        return {
            "table": table_name,
            "description": config["description"],
            "status": "created",
            "record_count": df.count()
        }

    finally:
        spark.stop()


def create_all_gold_tables():
    results = []
    for table_name in GOLD_TABLE_CONFIG.keys():
        try:
            result = create_gold_table(table_name)
            results.append(result)
        except Exception as e:
            results.append({
                "table": table_name,
                "status": "failed",
                "error": str(e)
            })
    return results