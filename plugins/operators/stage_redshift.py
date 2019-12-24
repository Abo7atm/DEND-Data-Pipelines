import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id=None,
                 s3_bucket=None,
                 insert_query=None,
                 redshift_conn_id=None,
                 aws_conn_id=None,
                 iam_role=None,
                 json_format=None,
                 table_name=None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket=s3_bucket
        self.insert_query=insert_query
        self.redshift_conn_id=redshift_conn_id
        self.aws_conn_id=aws_conn_id
        self.iam_role=iam_role
        self.json_format=json_format
        self.table_name=table_name
    

    def __is_file_json(file_name):
        end = len('json')
        if file_name[-end:].lower() == 'json'.lower():
            return True
        return False


    def execute(self, context):
        self.log.info(f"Beginning to stage {self.table_name}")
        
        # initialize hooks
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        redshift = PostgresHook(self.redshift_conn_id)
        
        # create tables
        self.log.info(f"executing -- create: {self.create_query}")
        self.log.info(f"executing -- insert: {self.insert_query}")
        self.log.info(f"executing -- iam_role: {self.iam_role}")
        self.log.info(f"executing {self.json_format}")
        self.log.info(f"executing {self.table_name}")
        redshift.run(self.create_query)
        
        # iterate through keys in bucket
        for key in s3_hook.list_keys(self.s3_bucket, prefix=self.s3_prefix):
            s3_file = f"s3://{s3_bucket}/{self.key}"
            
            if __is_file_json(s3_file):
                self.log.info(f"Copying from {s3_file}")
                self.log.info(insert_query.format(s3_file, self.iam_role))
                redshift.run(insert_query.format(s3_file, self.iam_role))

