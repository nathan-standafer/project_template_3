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
# CREATE TABLE public.users (
# 	userid int4 NOT NULL,
# 	first_name varchar(256),
# 	last_name varchar(256),
# 	gender varchar(256),
# 	"level" varchar(256),
# 	CONSTRAINT users_pkey PRIMARY KEY (userid)
# );

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
# CREATE TABLE public.songs (
# 	songid varchar(256) NOT NULL,
# 	title varchar(256),
# 	artistid varchar(256),
# 	"year" int4,
# 	duration numeric(18,0),
# 	CONSTRAINT songs_pkey PRIMARY KEY (songid)
# );

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
# CREATE TABLE public.artists (
# 	artistid varchar(256) NOT NULL,
# 	name varchar(256),
# 	location varchar(256),
# 	lattitude numeric(18,0),
# 	longitude numeric(18,0)
# );

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
# CREATE TABLE public."time" (
# 	start_time timestamp NOT NULL,
# 	"hour" int4,
# 	"day" int4,
# 	week int4,
# 	"month" varchar(256),
# 	"year" int4,
# 	weekday varchar(256),
# 	CONSTRAINT time_pkey PRIMARY KEY (start_time)
# ) ;

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id='redshift',
                 aws_connection_credentials_id='aws_credentials',
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(**kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

        self.redshift_conn_id = redshift_conn_id
        self.aws_connection_credentials_id = aws_connection_credentials_id
        self.task_id = kwargs['task_id']

    def execute(self, context):

        if self.task_id == 'Load_user_dim_table':
            s3_load_sql = self.user_table_insert
        elif self.task_id == 'Load_song_dim_table':
            s3_load_sql = self.song_table_insert
        elif self.task_id == 'Load_artist_dim_table':
            s3_load_sql = self.artist_table_insert
        elif self.task_id == 'Load_time_dim_table':
            s3_load_sql = self.time_table_insert
        else:
            raise RuntimeError('task_id must be Stage_events or Stage_songs')

        self.log.info('Copying log data from S3 to staging table with assigned task: {}, with query: {}'.format(self.task_id, s3_load_sql))

        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(s3_load_sql)
