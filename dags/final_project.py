from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

default_args = {
    'owner': 'nathan-standafer',
    'start_date': pendulum.now(),
    'retries': 3,                           # On failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=5),    # Retries happen every 5 minutes
    'catchup': False,                       # Catchup is turned off
    'email_on_retry': False,                # Do not email on retry
    'depends_on_past': False                # The DAG does not have dependencies on past runs
}

@dag(
    default_args=default_args,
    description='Load data from S3 and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        redshift_conn_id='redshift',
        aws_connection_credentials_id='aws_credentials',
        task_id='Stage_events',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        redshift_conn_id='redshift',
        aws_connection_credentials_id='aws_credentials',
        task_id='Stage_songs',
    )

    load_songplays_table = LoadFactOperator(
        redshift_conn_id='redshift',
        aws_connection_credentials_id='aws_credentials',
        full_delete_load=True,
        task_id='Load_songplays_fact_table',
    )

    load_user_dimension_table = LoadDimensionOperator(
        redshift_conn_id='redshift',
        aws_connection_credentials_id='aws_credentials',
        task_id='Load_user_dim_table',
        full_delete_load=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        redshift_conn_id='redshift',
        aws_connection_credentials_id='aws_credentials',
        task_id='Load_song_dim_table',
        full_delete_load=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        redshift_conn_id='redshift',
        aws_connection_credentials_id='aws_credentials',
        task_id='Load_artist_dim_table',
        full_delete_load=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        redshift_conn_id='redshift',
        aws_connection_credentials_id='aws_credentials',
        task_id='Load_time_dim_table',
        full_delete_load=True
    )

    quality_check_queries = {
        'staging_events': 'select count(*) from staging_events',
        'staging_songs': 'select count(*) from staging_songs',
        'artists': 'select count(*) from artists',
        'songplays': 'select count(*) from songplays',
        'songs': 'select count(*) from songs',
        'time': 'select count(*) from time',
        'users': 'select count(*) from users'
    }

    run_quality_checks = DataQualityOperator(
        redshift_conn_id='redshift',
        aws_connection_credentials_id='aws_credentials',
        task_id='Run_data_quality_checks',
        quality_check_queries=quality_check_queries
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift  >> load_songplays_table

    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_song_dimension_table   >> run_quality_checks
    load_user_dimension_table   >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table   >> run_quality_checks

    run_quality_checks >> end_operator

final_project_dag = final_project()