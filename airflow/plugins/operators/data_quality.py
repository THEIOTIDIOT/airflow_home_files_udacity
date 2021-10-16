from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 tables=[],
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables
        self.dq_checks=dq_checks
        
    def execute(self, context):
        
        self.log.info("Starting data quality checks.....")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        table_errors = []
        
        # Checking for valid tables
        for item in self.dq_checks:
            table = item.get('table_name')
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                message = f"Data quality check failed. {table} returned no results"
                raise ValueError(message)
                table_errors.append(message)
            num_records = records[0][0]
            if num_records < 1:
                message = f"Data quality check failed. {table} contained 0 rows"
                raise ValueError(message)
                table_errors.append(message)
                
        # Checking for nulls
        for item in self.dq_checks:
            table = item.get('table_name')
            col = item.get('column_name')
            sql = item.get('test_sql')
            exp_results = item.get('expected_result')
            
            records = redshift.get_records(sql.format(table,col))
            
            if records[0] != exp_results:
                message = f"Data quality check failed. In the table {table}, {col} contains nulls"
                raise ValueError(message)
                table_errors.append(message)
        
        num_of_errors = len(table_errors)
        if num_of_errors > 0:
            self.log.info("{} data quality errors found".format(num_of_errors))
            self.log.info(table_errors)
            self.log.info("{} data quality checks failed".format(num_of_errors))
        else:
            self.log.info("All data quality checks passed")
        
        
        