from airflow.hooks.postgres_hook import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    
    LOAD_FACTS_TABLE_SQL = """
    insert into songplays (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)
    (
        select 
            se.sessionid || '~' || se.iteminsession,
            DATEADD(ms, se.ts, 'epoch'),
            se.userid,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionid,
            se.location,
            se.useragent
        from 
            staging_songs ss
        join staging_events se on se.song = ss.title and se.artist = ss.artist_name
    )
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id='redshift',
                 aws_connection_credentials_id='aws_credentials',
                 **kwargs):

        super(LoadFactOperator, self).__init__(**kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_connection_credentials_id = aws_connection_credentials_id
        self.task_id = kwargs['task_id']

    def execute(self, context):
        self.log.info("Loading songplays facts table for task_id: {}".format(self.task_id))
        self.log.info('Loading songplays facts table using query: {}'.format(self.LOAD_FACTS_TABLE_SQL))

        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(self.LOAD_FACTS_TABLE_SQL)
