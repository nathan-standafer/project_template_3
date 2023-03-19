from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    test_sql = 'select count(*) from {table_name}'
    table_names = ['staging_events', 'staging_songs', 'artists', 'songplays', 'songs', 'time', 'users']

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 aws_connection_credentials_id: str,
                 **kwargs):

        super(DataQualityOperator, self).__init__(**kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_connection_credentials_id = aws_connection_credentials_id
        self.task_id = kwargs['task_id']

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table_name in self.table_names:

            test_sql = self.test_sql.format(table_name = table_name)
            self.log.info(f"validating table '{table_name}' using sql: {test_sql}")

            records = redshift_hook.get_records(test_sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. '{table_name}' returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. '{table_name}' contained 0 rows")
            self.log.info(f"Data quality on table '{table_name}' check passed with {records[0][0]} records")