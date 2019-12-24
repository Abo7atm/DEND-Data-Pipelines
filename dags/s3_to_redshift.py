from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.models import Variable
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Abdulellah',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('s3_to_redshift',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_bucket=Variable.get('s3_bucket'),
    s3_prefix=Variable.get('log_prefix'),
    insert_query=SqlQueries.staging_copy,
    redshift_conn_id='redshift',
    aws_conn_id='aws_credintials', 
    iam_role=Variable.get('iam_role'),
    json_format='log_json_path.json',
    table_name='staging_events'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_bucket=Variable.get('s3_bucket'),
    s3_prefix=Variable.get('song_prefix'),
    insert_query=SqlQueries.staging_copy,
    redshift_conn_id='redshift',
    aws_conn_id='aws_credintials',
    iam_role=Variable.get('iam_role'),
    json_format='auto',
    table_name='staging_songs'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    insert_query=SqlQueries.songplays_table_insert,
    table_name='songplays',
    redshift_conn_id='redshift'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    insert_query=SqlQueries.users_table_insert,
    table_name='users',
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    insert_query=SqlQueries.songs_table_insert,
    table_name='songs',
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    insert_query=SqlQueries.artists_table_insert,
    table_name='artists',
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    insert_query=SqlQueries.time_table_insert,
    table_name='time',
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
    conn_id='redshift',
    tables=['users', 'songs', 'artists', 'time', 'songplays']
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table 
load_songplays_table >> load_song_dimension_table 
load_songplays_table >> load_artist_dimension_table 
load_songplays_table >> load_time_dimension_table 

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
