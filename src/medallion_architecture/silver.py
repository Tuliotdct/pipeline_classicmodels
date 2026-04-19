import boto3
from pyspark.sql import functions as F
from src.connections.spark_connections import create_spark_session
from src.config.silver.silver_config import SILVER_TABLE_CONFIG
from dotenv import load_dotenv
import os

load_dotenv()

bucket = os.getenv("S3_BUCKET_NAME")
s3 = boto3.client('s3')
glue = boto3.client('glue')


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


def _silver_table_exists(spark, table_name):
    try:
        return spark.catalog.tableExists(f"glue_catalog.silver.{table_name}")
    except Exception as e:
        if "NotFoundException" in str(e) or "does not exist" in str(e).lower():
            try:
                glue.delete_table(DatabaseName="silver", Name=table_name)
            except glue.exceptions.EntityNotFoundException:
                pass
            return False
        raise


def _join_condition(alias_target, alias_source, primary_key):
    conditions = []
    for k in primary_key:
        conditions.append(f"{alias_target}.{k} = {alias_source}.{k}")
    return " AND ".join(conditions)


def silver_scd2(table_name, primary_key, execution_date, spark):
    silver_table = f"glue_catalog.silver.{table_name}"
    df_new = spark.read.parquet(f"s3a://{bucket}/bronze/{table_name}/{execution_date}")

    if not _silver_table_exists(spark, table_name):
        df_new = df_new \
            .withColumn("effective_date", F.lit(execution_date)) \
            .withColumn("end_date", F.lit(None).cast("string")) \
            .withColumn("is_current", F.lit(True))
        df_new.writeTo(silver_table) \
            .tableProperty("write.format.default", "parquet") \
            .create()
        return {"table": table_name, "execution_date": execution_date, "status": "created"}

    df_new.createOrReplaceTempView("bronze_snapshot")
    join_on = _join_condition("target", "source", primary_key)
    data_cols = [c for c in df_new.columns]
    changed_condition = " OR ".join(
        [f"target.{c} <> source.{c}" for c in data_cols]
    )

    spark.sql(f"""
        MERGE INTO {silver_table} AS target
        USING bronze_snapshot AS source
        ON {join_on} AND target.is_current = true
        WHEN MATCHED AND ({changed_condition})
            THEN UPDATE SET target.end_date = '{execution_date}', target.is_current = false
    """)

    spark.sql(f"""
        INSERT INTO {silver_table}
        SELECT source.*, '{execution_date}' AS effective_date, null AS end_date, true AS is_current
        FROM bronze_snapshot source
        INNER JOIN {silver_table} target
            ON {join_on}
        WHERE target.is_current = false AND target.end_date = '{execution_date}'
    """)

    spark.sql(f"""
        INSERT INTO {silver_table}
        SELECT source.*, '{execution_date}' AS effective_date, null AS end_date, true AS is_current
        FROM bronze_snapshot source
        LEFT JOIN {silver_table} target
            ON {join_on}
        WHERE target.{primary_key[0]} IS NULL
    """)

    return {"table": table_name, "execution_date": execution_date, "status": "success"}


def silver_append(table_name, primary_key, execution_date, spark):
    silver_table = f"glue_catalog.silver.{table_name}"
    df_new = spark.read.parquet(f"s3a://{bucket}/bronze/{table_name}/{execution_date}")

    if not _silver_table_exists(spark, table_name):
        df_new.writeTo(silver_table) \
            .tableProperty("write.format.default", "parquet") \
            .create()
        return {"table": table_name, "execution_date": execution_date, "status": "created"}

    df_new.createOrReplaceTempView("bronze_snapshot")
    join_on = _join_condition("target", "source", primary_key)

    spark.sql(f"""
        INSERT INTO {silver_table}
        SELECT source.*
        FROM bronze_snapshot source
        LEFT JOIN {silver_table} target
            ON {join_on}
        WHERE target.{primary_key[0]} IS NULL
    """)

    return {"table": table_name, "execution_date": execution_date, "status": "success"}


def silver_scd1(table_name, primary_key, execution_date, spark):
    silver_table = f"glue_catalog.silver.{table_name}"
    df_new = spark.read.parquet(f"s3a://{bucket}/bronze/{table_name}/{execution_date}")

    if not _silver_table_exists(spark, table_name):
        df_new.writeTo(silver_table) \
            .tableProperty("write.format.default", "parquet") \
            .create()
        return {"table": table_name, "execution_date": execution_date, "status": "created"}

    df_new.createOrReplaceTempView("bronze_snapshot")
    join_on = _join_condition("target", "source", primary_key)

    spark.sql(f"""
        MERGE INTO {silver_table} AS target
        USING bronze_snapshot AS source
        ON {join_on}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    return {"table": table_name, "execution_date": execution_date, "status": "success"}


def create_silver_table(table_name):
    config = SILVER_TABLE_CONFIG[table_name]
    strategy = config["strategy"]
    primary_key = config["primary_key"]
    execution_date = last_execution_date(table_name)

    spark = create_spark_session()
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.silver LOCATION 's3a://{bucket}/silver'")

        if strategy == "scd2":
            return silver_scd2(table_name, primary_key, execution_date, spark)
        elif strategy == "append":
            return silver_append(table_name, primary_key, execution_date, spark)
        elif strategy == "scd1":
            return silver_scd1(table_name, primary_key, execution_date, spark)
        else:
            raise ValueError(f"Unknown strategy '{strategy}' for table '{table_name}'")
    finally:
        spark.stop()
