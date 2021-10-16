from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    truncate_sql="TRUNCATE TABLE {}; \
                  INSERT INTO {} \
                  {}"
    
    insert_sql="INSERT INTO {} \
                {}"
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 do_truncate_insert=False,
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.do_truncate_insert=do_truncate_insert
        self.sql=sql

    def execute(self, context):
        
        sql=""
        
        if self.do_truncate_insert:
            self.log.info("Deleting all previous records in the {} table".format(self.table))
            sql=LoadDimensionOperator.truncate_sql.format(self.table, self.table, self.sql)
        else:
            sql=LoadDimensionOperator.insert_sql.format(self.table, self.sql)
            
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Inserting records into the {} table".format(self.table))
        redshift.run(sql)
