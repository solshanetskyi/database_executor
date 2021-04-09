###### Usage

Before running the tool, create 'credentials.json' in the root folder with the following content:

`{
  "prod_combined_replica_user_name": "",
  "prod_combined_replica_password": "",
  "prod_read_replica_user_name": "",
  "prod_read_replica_password": "",
  "staging_user_name": "",
  "staging_password": ""
}`

Populate your credentials to the corresponding data stores in this file.

Initialize DatabaseExecutor with a particular environment (Combined Replica, Read Replica, Staging cluster 1, Staging cluster 2).

Please note that Combined Replica requires VPN access, the rest of the environments require open tunnels. 