from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 aws_connection_credentials_id: str,
                 quality_check_queries: dict,
                 **kwargs):

        super(DataQualityOperator, self).__init__(**kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_connection_credentials_id = aws_connection_credentials_id
        self.task_id = kwargs['task_id']
        self.quality_check_queries = quality_check_queries

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        #for table_name in self.table_names:
        for table_identifier_key in self.quality_check_queries.keys():

            test_sql = self.quality_check_queries[table_identifier_key]
            self.log.info(f"validating table '{table_identifier_key}' using sql: {test_sql}")

            records = redshift_hook.get_records(test_sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. '{table_identifier_key}' returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. '{table_identifier_key}' contained 0 rows")
            self.log.info(f"Data quality on table '{table_identifier_key}' check passed with {records[0][0]} records")