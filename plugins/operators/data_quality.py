from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id=None,
                 tables=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.tables=tables

    def execute(self, context):
        # self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(self.conn_id)

        for table in tables:
            r = redshift.run(f"COUNT * FROM {table}")
            if len(r) < 1 or len(r[0]) < 1:
                raise ValueError(f"No records present in destinaiton table {table}")
            self.log.info(f"Data quality on table {table} check passed.")

