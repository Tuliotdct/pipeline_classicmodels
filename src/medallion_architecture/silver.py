import boto3
from src.connections.spark_connections import create_spark_session, spark_jdbc_connection
from dotenv import load_dotenv
import os


load_dotenv()

bucket = os.getenv("S3_BUCKET_NAME")
s3 = boto3.client('s3')

def bronze_tables():

    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix='bronze/', Delimiter='/')

    list_bronze_tables = []
    for page in page_iterator:
        if 'CommonPrefixes' not in page:
            continue
        
        for obj in page['CommonPrefixes']:
            filter_bronze_tables = obj['Prefix'].split('/')[1]
            list_bronze_tables.append(filter_bronze_tables)

    return list_bronze_tables


def last_execution_date(table_name=None):
    
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=f'bronze/{table_name}/', Delimiter='/')

    list_execution_date = []
    for page in page_iterator:
        if 'CommonPrefixes' not in page:
            continue
        
        for obj in page['CommonPrefixes']:
            filter_execution_date = obj['Prefix'].split('/')[2]
            list_execution_date.append(filter_execution_date)

    list_execution_date = max(list_execution_date) 

    return list_execution_date


def create_current_silver_for_table(table_name):
    current_execution_date = last_execution_date(table_name)
    spark = create_spark_session()
    try:
        df = spark.read.parquet(f"s3a://{bucket}/bronze/{table_name}/{current_execution_date}")
        df.write.mode("overwrite").parquet(f"s3a://{bucket}/silver_current/{table_name}")
        return {"table": table_name, "execution_date": current_execution_date, "records": df.count(), "status": "success"}
    finally:
        spark.stop()


