from airflow.sdk import dag, task, TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum
from src.medallion_architecture.silver import create_silver_table
from src.config.silver.silver_config import SILVER_TABLE_CONFIG


@dag(
    dag_id='silver_dag',
    schedule='@daily',
    start_date=pendulum.datetime(2026, 2, 7),
    catchup=False,
    tags=['pipeline', 'medallion architecture', 'silver'],
    max_active_tasks=1,
)
def silver_dag():

    start = EmptyOperator(task_id='start')

    with TaskGroup(group_id='silver_jobs') as silver_group:
        @task
        def load_single_table_silver(table_name):
            return create_silver_table(table_name=table_name)

        for table in SILVER_TABLE_CONFIG.keys():
            silver_task = load_single_table_silver.override(task_id=f'{table}')(table_name=table)
            silver_task

    end = EmptyOperator(task_id='end')

    start >> silver_group >> end


silver_dag()
