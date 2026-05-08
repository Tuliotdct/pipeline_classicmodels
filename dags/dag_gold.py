from airflow.sdk import dag, task, TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum
from src.medallion_architecture.gold import create_gold_table, create_all_gold_tables
from src.config.gold.gold_config import GOLD_TABLE_CONFIG


@dag(
    dag_id='gold_dag',
    schedule='@daily',
    start_date=pendulum.datetime(2026, 2, 7),
    catchup=False,
    tags=['pipeline', 'medallion architecture', 'gold'],
    max_active_tasks=1,
)
def gold_dag():

    start = EmptyOperator(task_id='start')

    with TaskGroup(group_id='gold_jobs') as gold_group:
        @task
        def load_single_table_gold(table_name):
            return create_gold_table(table_name=table_name)

        for table in GOLD_TABLE_CONFIG.keys():
            gold_task = load_single_table_gold.override(task_id=f'{table}')(table_name=table)
            gold_task

    end = EmptyOperator(task_id='end')

    start >> gold_group >> end


gold_dag()