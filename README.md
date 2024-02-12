Glue ETL with Airflow
ETL Process with Airflow
This repository contains code for an ETL (Extract, Transform, Load) process implemented using Apache Airflow.

Overview
The ETL process consists of two main components:

ETL DAG: Defines the workflow and scheduling for the ETL process.
ETL Process Task: Executes the actual ETL logic on the specified data source.
Components
ETL DAG
The etl_dag.py file defines the Airflow DAG for orchestrating the ETL process. It schedules the execution of the ETL process task on a daily basis.

DAG Configuration
DAG ID: etl_dag
Start Date: February 10, 2024
Schedule Interval: Daily
ETL Process Task
The etl_process.py file contains the Python script for the ETL process task. It performs the following steps:

Connects to the data source (e.g., an S3 bucket).
Extracts data from the source.
Transforms the data as required.
Loads the transformed data into the destination.
Task Details
Task ID: etl_process_task
Execution: Invokes the main() function defined in etl_process.py.

The config.json is given if we are not using Manged workflow Apache Airflow(MWAA) by Amazon, we need to access AWS glue with Apache Airflo0w using access keys, But here I am using MWAA.


Requirements
Python 3
Apache Airflow
Pyspark
Glue 4.0 - Supports spark 3.3

Dependencies
boto3: Python SDK for AWS services (required for S3 operations).

The input file in s3 : 
Bucket - Folder - files(In This ETL I have taken (CSV, txt, parquet)
The Output is also saved as the same:
Output_bucket- Folder -File
