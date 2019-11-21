from database_executor import DatabaseExecutor, Environment

executor = DatabaseExecutor(Environment.PROD_CR)
executor.execute_sharded_query('custom_query.sql', 'production_systems_cr.csv')
