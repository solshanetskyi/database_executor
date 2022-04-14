from database_executor import DatabaseExecutor, Environment

executor = DatabaseExecutor(Environment.PROD)
executor.execute_sharded_query('custom_query.sql', 'temp.csv')