from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 insert_query=None,
                 redshift_conn_id=None,
                 table_name=None,
                 trunc=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.insert_query=insert_query
        self.table_name=table_name
        self.redshift_conn_id=redshift_conn_id


    def execute(self, context):
        if trunc:
            redshift.run(f"TRUNCATE {self.table_name};")

        redshift = PostgresHook(self.redshift_conn_id)
        redshift.run("""
                    INSERT INTO {}
                    {};""".format(self.table_name, self.insert_query))
 
