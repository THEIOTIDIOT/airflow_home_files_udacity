from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, \
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

data_quality_checks = [{'table_name': 'artists', 'column_name': 'artistid', \
                        'test_sql': 'SELECT COUNT(*) FROM {} WHERE {} is NULL', \
                        'expected_result': 0}, \
                      {'table_name': 'songplays', 'column_name': 'playid', \
                        'test_sql': 'SELECT COUNT(*) FROM {} WHERE {} is NULL', \
                        'expected_result': 0}, \
                      {'table_name': 'staging_events', 'column_name': '*', \
                        'test_sql': 'SELECT COUNT(*) FROM {} WHERE {} is NULL', \
                        'expected_result': 0}, \
                      {'table_name': 'staging_songs', 'column_name': '*', \
                        'test_sql': 'SELECT COUNT(*) FROM {} WHERE {} is NULL', \
                        'expected_result': 0}, \
                      {'table_name': 'time', 'column_name': 'start_time', \
                        'test_sql': 'SELECT COUNT(*) FROM {} WHERE {} is NULL', \
                        'expected_result': 0}, \
                      {'table_name': 'users', 'column_name': 'userid', \
                        'test_sql': 'SELECT COUNT(*) FROM {} WHERE {} is NULL', \
                        'expected_result': 0}, \
                      {'table_name': 'songs', 'column_name': 'songid', \
                        'test_sql': 'SELECT COUNT(*) FROM {} WHERE {} is NULL', \
                        'expected_result': 0}]

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          catchup=False,
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_events",
    s3_bucket="udacity-dend",
    #s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    s3_key="log_data/2018/11",
    aws_credentials_id="aws_credentials",
    json_format="s3://udacity-dend/log_json_path.json",
    region="us-west-2"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    aws_credentials_id="aws_credentials",
    json_format="auto",
    region="us-west-2"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    do_truncate_insert=False,
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    do_truncate_insert=False,
    sql=SqlQueries.song_table_insert
    
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    do_truncate_insert=False,
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    do_truncate_insert=True,
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    test_sql=data_quality_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
 
