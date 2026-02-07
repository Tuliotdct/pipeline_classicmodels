from pyspark.sql import SparkSession
from .aws_secrets import get_secret

def create_spark_session():
        
    spark = SparkSession.builder \
    .appName("classicmodels-extract") \
    .config("spark.jars.packages",
            "org.postgresql:postgresql:42.7.1,"
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.767") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider") \
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

def create_bronze_for_table(table_name):

    spark = create_spark_session()

    jdbc_url, props = spark_jdbc_connection()

    read_db_tables = spark.read.jdbc(url=jdbc_url, table=f'{table_name}', properties=props)

    read_db_tables.write.mode("overwrite").parquet(f"s3a://lakehouse-classicmodels/bronze/{table_name}")

    return read_db_tables
