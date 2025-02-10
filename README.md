# Data Warehouse
Second project provided by Udacity in the "Data Engineering with AWS" course. The aim of the project is to create Star Schema, 

### The project includes the following files:
- *create_table.py* - where the star schema is created, it consists from fact and dimension tables in Redshift.
- *etl.py* - where data gets loaded from S3 into staging tables on redshift and then gets processed in analytics tables in Redshift.
- *sql_queries.py* - where SQL statements are defined, which then get imported into the *create_table.py* and *etl.py* files.
  
### Steps:
1. Design schemas for the fact and dimension tables.
2. Create these fact and dimension tables in the *sql_queris.py* using CREATE statement.
3. In *create_tables.py* write logic to connect to the database and create these tables there.
4. Write SQL DROP statements for the above tables, add these statements at the start of the *create_tables.py*. This way, if the file will be run again, the database will be reset and ETL pipelines can be tested, without getting an error.
5. After launching Redshift cluster, create an IAM role that has read access to S3.
6. Add information about redshift database and IAM role to *dwh.cfg*.
7. Test that everything is running correctly by running *create_tables.p*y and checking the table schemas in the redshift database. For the testing purposes, Query Editor in the AWS Redshift console can be used.
8. Implement the logic in *etl.py* to load data from S3 to staging tables on Redshift.
9. Implement the logic in *etl.py* to load data from staging tables to analytics tables on Redshift.
10. Test by running *etl.py* after running *create_tables.py* and running the analytic queries on the created Redshift database to compare the outputs with the expected results.
11. Delete the redshift cluster when finished to avoid extreme costs.
12. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
13. State and justify your database schema design and ETL pipeline.
14. Provide some queries and results for song play analysis.

### Relevant instructions:
#### Create an IAM Role
#### Create Security Group
#### Create an IAM User
#### Launch a Redshift Cluster
#### Delete a Redshift Cluster
#### ETL in Redshift
   
