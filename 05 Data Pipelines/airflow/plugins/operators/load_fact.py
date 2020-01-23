from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    load_sql = """
        INSERT INTO {}
        {};
        COMMIT;
    """

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id,
                 sql,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from fact Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Loading fact table in Redshift")
        formatted_sql = LoadFactOperator.load_sql.format(
            self.table,
            self.sql
        )
        redshift.run(formatted_sql)
