from airflow.hooks.postgres_hook import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    COPY_SQL = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{{}}'
    SECRET_ACCESS_KEY '{{}}'
    format as json 'auto ignorecase' compupdate off 
    """

    COPY_LOG_DATA_SQL = COPY_SQL.format(
        "staging_events",
        "s3://nas-udacity-data-pipelines-2/log_data"
    )

    COPY_SONG_DATA_SQL = COPY_SQL.format(
        "staging_songs",
        "s3://nas-udacity-data-pipelines-2/song_data"
    )

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 aws_connection_credentials_id: str,
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(**kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_connection_credentials_id = aws_connection_credentials_id

        self.task_id = kwargs['task_id']

        
    def execute(self, context):

        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_connection_credentials_id)
        self.log.info("Executing staging sql for task_id: {}".format(self.task_id))

        if self.task_id == 'Stage_events':
            s3_log_data_staging_sql = self.COPY_LOG_DATA_SQL.format(aws_connection.login, aws_connection.password)
        elif self.task_id == 'Stage_songs':
            s3_log_data_staging_sql = self.COPY_SONG_DATA_SQL.format(aws_connection.login, aws_connection.password)
        else:
            raise RuntimeError('task_id must be Stage_events or Stage_songs')

        self.log.info('Copying log data from S3 to staging table with assigned task: {}, with query: {}'.format(self.task_id, s3_log_data_staging_sql))

        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(s3_log_data_staging_sql.format(aws_connection.login, aws_connection.password))

