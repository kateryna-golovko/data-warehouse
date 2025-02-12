# Data Warehouse
Second project provided by Udacity in the "Data Engineering with AWS" course. The aim of the project is to create Star Schema, 

### The project includes the following files:
- *create_table.py* - where the star schema is created, it consists from fact and dimension tables in Redshift.
- *etl.py* - where data gets loaded from S3 into staging tables on redshift and then gets processed in analytics tables in Redshift.
- *sql_queries.py* - where SQL statements are defined, which then get imported into the *create_table.py* and *etl.py* files.
  
### Steps:
1. Design schemas for the fact and dimension tables.
2. Create these fact and dimension tables in the *sql_queris.py* using CREATE statement.
3. Add SQL DROP statements for the fact, dim and staging tables in the *sql_queris.py*.
4. In *create_tables.py* write logic to connect to the database and create these tables there. Do the following steps:
   - Create a cluster in Redshift
   - Note down the connection information to later add to *create_tables.py*
   - Create a security group to ensure that the Redshift cluster’s security group allows inbound connections from your IP address - need to modify the security group   
     settings to allow connections on port 5439 (Redshift’s default port) from your IP address or your VPC.
6. Write SQL DROP statements for the above tables, add these statements at the start of the *create_tables.py*. This way, if the file will be run again, the database will be reset and ETL pipelines can be tested, without getting an error.
7. After launching Redshift cluster, create an IAM role that has read access to S3.
8. Add information about redshift database and IAM role to *dwh.cfg*.
9. Test that everything is running correctly by running *create_tables.p*y and checking the table schemas in the redshift database. For the testing purposes, Query Editor in the AWS Redshift console can be used.
10. Implement the logic in *etl.py* to load data from S3 to staging tables on Redshift.
11. Implement the logic in *etl.py* to load data from staging tables to analytics tables on Redshift.
12. Test by running *etl.py* after running *create_tables.py* and running the analytic queries on the created Redshift database to compare the outputs with the expected results.
13. Delete the redshift cluster when finished to avoid extreme costs.
14. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
15. State and justify your database schema design and ETL pipeline.
16. Provide some queries and results for song play analysis.

*Note that when Redshift cluter is deleted all the created tables get deleted as well.*

### Relevant instructions:
#### Create an IAM Role
#### Create Security Group
#### Create an IAM User
#### Launch a Redshift Cluster
#### Delete a Redshift Cluster
#### ETL in Redshift
   
