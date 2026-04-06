import boto3
from src.connections.spark_connections import create_spark_session
from dotenv import load_dotenv
import os

load_dotenv()

bucket = os.getenv("S3_BUCKET_NAME")
s3 = boto3.client('s3')


def bronze_tables():
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix='bronze/', Delimiter='/')

    tables = []
    for page in page_iterator:
        if 'CommonPrefixes' not in page:
            continue
        for obj in page['CommonPrefixes']:
            tables.append(obj['Prefix'].split('/')[1])
    return tables


def last_execution_date(table_name):
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=f'bronze/{table_name}/', Delimiter='/')

    dates = []
    for page in page_iterator:
        if 'CommonPrefixes' not in page:
            continue
        for obj in page['CommonPrefixes']:
            dates.append(obj['Prefix'].split('/')[2])
    return max(dates)


def create_silver_table(table_name):
    execution_date = last_execution_date(table_name)
    spark = create_spark_session()
    try:
        df = spark.read.parquet(f"s3a://{bucket}/bronze/{table_name}/{execution_date}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.silver LOCATION 's3a://{bucket}/silver'")
        df.writeTo(f"glue_catalog.silver.{table_name}") \
          .tableProperty("write.format.default", "parquet") \
          .createOrReplace()
        return {"table": table_name, "execution_date": execution_date, "status": "success"}
    finally:
        spark.stop()


print(create_silver_table("customers"))