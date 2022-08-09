###### Usage

Before running the tool, create 'credentials.json' in the root folder with the following content:

```
{
  "prod_read_replica_user_name": "",
  "prod_read_replica_password": "",
  "staging_user_name": "",
  "staging_password": ""
}
```

Populate your credentials to the corresponding data stores in this file.

Initialize DatabaseExecutor with a particular environment (Production Read Replica, Staging cluster 1, Staging cluster 2).

`main.py` contains an example of the usage:

```
executor = DatabaseExecutor(Environment.PROD)
executor.execute_sharded_query('custom_query.sql', 'temp.csv')
```