from airflow.sdk import dag, task, TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum
from src.medallion_architecture.bronze import create_bronze_for_table
import pandas as pd

tables = pd.read_csv('src/config/db_tables.txt', header=None).squeeze().tolist()

print(tables)
@dag(
    dag_id='bronze_dag',
    schedule='@daily',
    start_date=pendulum.datetime(2026,2,7),
    catchup=False,
    tags=['pipeline','medallion architecture', 'bronze'],
    max_active_tasks=1
)

def bronze_dag():

    start = EmptyOperator(task_id = 'start')

    with TaskGroup(group_id='bronze_jobs') as bronze_group:
        @task
        def load_single_table_bronze(table_name):
            return create_bronze_for_table(table_name)
        
        tables = pd.read_csv('src/config/db_tables.txt').squeeze().tolist()

        for table in tables:
            bronze_task = load_single_table_bronze.override(task_id=f'{table}')(table_name=table)
            bronze_task

    end = EmptyOperator(task_id = 'end')

    start >> bronze_group >> end


bronze_dag()
