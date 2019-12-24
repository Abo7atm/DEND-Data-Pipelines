from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from helpers.create_tables_queries import queries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'Abdulellah',
    'start_date': datetime.utcnow(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('create_tables',
          default_args=default_args,
          description='Create Tables in Redshift',
          schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_tables = PostgresOperator(
    task_id='Create_table',
    dag=dag,
    sql=queries,
    postgres_conn_id='redshift',
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_tables
create_tables >> end_operator