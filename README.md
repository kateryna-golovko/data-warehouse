# Data Warehouse
Second project provided by Udacity in the "Data Engineering with AWS" course. The aim of the project is to create Star Schema, 

The project includes the following files:
- *create_table.py* - where the star schema is created, it consists from fact and dimension tables in Redshift.
- *etl.py* - where data gets loaded from S3 into staging tables on redshift and then gets processed in analytics tables in Redshift.
- *sql_queries.py* - where SQL statements are defined, which then get imported into the *create_table.py* and *etl.py* files.
  
Steps:
1. Design schemas for the fact and dimension tables.
2. Create these fact and dimension tables in the *sql_queris.py* using CREATE statement.
3. In *create_tables.py* write logic to connect to the database and create these tables there.
   
