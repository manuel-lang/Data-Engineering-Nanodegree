from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 table,
                 query,
                 truncate = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.query = query
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=conn_id)
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        redshift.run(f"INSERT INTO {self.table} {self.query}")
