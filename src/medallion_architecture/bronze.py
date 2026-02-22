from src.connections.spark_connections import create_spark_session, spark_jdbc_connection

def create_bronze_for_table(table_name, execution_date=None):

    spark = create_spark_session()

    jdbc_url, props = spark_jdbc_connection()

    read_db_tables = spark.read.jdbc(url=jdbc_url, table=f'{table_name}', properties=props)

    read_db_tables.write.mode("overwrite").parquet(f"s3a://lakehouse-classicmodels/bronze/{table_name}/{execution_date}")

    count = read_db_tables.count()
    
    spark.stop()
    
    return {"table": table_name, "records": count, "status": "success"}
