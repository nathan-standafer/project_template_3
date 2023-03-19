from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    user_table_insert = """
    insert into users (userid, first_name, last_name, gender, level)
    (
        select distinct
            se.userid,
            se.firstname,
            se.lastname,
            se.gender,
            se.level
        from 
            staging_events se
        where
            se.userid is not null
    )
    """

    user_table_delete = "delete from users"

    song_table_insert = """
    insert into songs (songid, title, artistid, year, duration)
    (
        select 
            ss.song_id,
            ss.title,
            ss.artist_id,
            ss.year,
            ss.duration
        from 
            staging_songs ss
    )
    """

    song_table_delete = "delete from songs"

    artist_table_insert = """
    insert into artists (artistid, name, location, lattitude, longitude)
    (
        select distinct
            ss.artist_id,
            ss.artist_name,
            ss.artist_location,
            ss.artist_latitude,
            ss.artist_longitude
        from 
            staging_songs ss
    )
    """

    artist_table_delete = "delete from artists"

    time_table_insert = """
    insert into time(start_time, hour, day, week, month, year, weekday)
    (
        select 
            DATEADD(ms, ts, 'epoch'),
            EXTRACT (HOUR FROM DATEADD(ms, ts, 'epoch')),
            EXTRACT (DAY FROM DATEADD(ms, ts, 'epoch')),
            EXTRACT (WEEK FROM DATEADD(ms, ts, 'epoch')),
            EXTRACT (MONTH FROM DATEADD(ms, ts, 'epoch')),
            EXTRACT (YEAR FROM DATEADD(ms, ts, 'epoch')),
            EXTRACT (WEEKDAY FROM DATEADD(ms, ts, 'epoch'))
        from staging_events
    )
    """

    time_table_delete = "delete from time"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 aws_connection_credentials_id: str,
                 full_delete_load: bool,
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(**kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_connection_credentials_id = aws_connection_credentials_id
        self.full_delete_load = full_delete_load
        self.task_id = kwargs['task_id']

    def execute(self, context):

        if self.task_id == 'Load_user_dim_table':
            load_sql = self.user_table_insert
            delete_sql = self.user_table_delete
        elif self.task_id == 'Load_song_dim_table':
            load_sql = self.song_table_insert
            delete_sql = self.song_table_delete
        elif self.task_id == 'Load_artist_dim_table':
            load_sql = self.artist_table_insert
            delete_sql = self.artist_table_delete
        elif self.task_id == 'Load_time_dim_table':
            load_sql = self.time_table_insert
            delete_sql = self.time_table_delete
        else:
            raise RuntimeError('task_id must be Stage_events or Stage_songs')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.full_delete_load:
            self.log.info("Deleting dimension data with sql: {}".format(delete_sql))
            redshift_hook.run(delete_sql)

        self.log.info('Loading dimension table for assigned task: {}, with query: {}'.format(self.task_id, load_sql))

        redshift_hook.run(load_sql)
