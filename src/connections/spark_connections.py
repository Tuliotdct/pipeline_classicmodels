from pyspark.sql import SparkSession
from .aws_secrets import get_secret
from dotenv import load_dotenv
import os

def create_spark_session():

    load_dotenv()

    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("REGION_NAME")
    bucket = os.getenv("S3_BUCKET_NAME")

    spark = SparkSession.builder \
    .appName("classicmodels-extract") \
    .config("spark.jars.packages",
            "org.postgresql:postgresql:42.7.1,"
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.767,"
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,"
            "org.apache.iceberg:iceberg-aws-bundle:1.10.1") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.driver.extraJavaOptions", f"-Daws.region={aws_region}") \
    .config("spark.executor.extraJavaOptions", f"-Daws.region={aws_region}") \
    .config("spark.sql.optimizer.excludedRules",
            "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts") \
    .config("spark.sql.catalog.glue_catalog.warehouse",
            f"s3a://{bucket}") \
    .getOrCreate()

    return spark

def spark_jdbc_connection():

    secret = get_secret()
    jdbc_url = f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}"

    props = {
        "user": secret['username'],
        "password": secret['password'],
        "driver": "org.postgresql.Driver"
    }

    return jdbc_url, props
