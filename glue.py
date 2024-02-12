import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import random
import os

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize S3 client
s3 = boto3.client('s3')

# Define function to list objects in S3 bucket
def list_objects(bucket_name, prefix=''):
    objects = []
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                objects.extend(page['Contents'])
    except Exception as e:
        print(f"Error listing objects in bucket {bucket_name}: {str(e)}")
    return objects

# print(objects)
# Define function to collect metadata from S3 objects
def collect_metadata(object_info):
    try:
        key = object_info['Key']
        size = object_info['Size']
        last_modified = object_info['LastModified']
        file_format = key.split('.')[-1]  # Extract file format from key
        return key, size, last_modified, file_format
    except Exception as e:
        print(f"Error collecting metadata for object {object_info['Key']}: {str(e)}")
        return None

# Define function to perform intelligent sampling from DataFrame
def intelligent_sampling(df, sample_size=0.1):
    try:
        sampled_df = df.sample(withReplacement=False, fraction=sample_size)
        return sampled_df
    except Exception as e:
        print(f"Error performing intelligent sampling: {str(e)}")
        return None
        
        

def process_file(bucket,file_format, file_name):
    # Read file from S3 based on file format
    if file_format == 'csv':
        df = spark.read.format("csv").option("header", "true").load(f"s3://{bucket}/{file_name}")
        # Perform ETL operations for CSV files
        # Example: df = df.withColumn(...)
    elif file_format == 'json':
        df = spark.read.format("json").load(f"s3://{bucket}/{file_name}")
        # Perform ETL operations for JSON files
        # Example: df = df.withColumn(...)
    elif file_format == 'parquet':
        df = spark.read.format("parquet").load(f"s3://{bucket}/{file_name}")
        # Perform ETL operations for Parquet files
        # Example: df = df.withColumn(...)
    elif file_format == 'txt':   
        df = spark.read.text(f"s3://{bucket}/{file_name}")
    else:
        # Handle other file formats if needed
        df = None
    return df
    
    
def write_to_s3(sampled_df,file_format,output_bucket,file_name):
    print(f"Writing {file_name}.{file_format}")
    if file_format == "parquet":
        sampled_df.write.format("parquet").mode("overwrite").save(f"s3://{output_bucket}/{file_name}")
        print(f"Succesfully saved to s3://{output_bucket}/{file_name}.")
    elif file_format == "txt":
        sampled_df.write.format("text").mode("overwrite").save(f"s3://{output_bucket}/{file_name}")
        print(f"Succesfully saved to s3://{output_bucket}/{file_name}.")
    elif file_format == "csv":
        sampled_df.write.format("csv").option("header", "true").mode("overwrite").save(f"s3://{output_bucket}/{file_name}")
        print(f"Succesfully saved to s3://{output_bucket}/{file_name}.")
    return True


def main():
    bucket_name = os.getenv('bucket_name', 'etltestsampling')
    output_bucket = os.getenv('output_bucket', 'etltestsampling-output')
    prefix = ''

    # List objects in the S3 bucket
    objects = list_objects(bucket_name, prefix)

    # Filter out None values (if any)
    objects = [obj for obj in objects if obj]

    # Create an RDD from the list of objects
    rdd = sc.parallelize(objects)

    # Apply the collect_metadata function to each object in the RDD
    metadata_rdd = rdd.map(collect_metadata)

    # Filter out None values (if any)
    metadata_rdd = metadata_rdd.filter(lambda x: x is not None)

    # Convert RDD to DataFrame
    metadata_df = metadata_rdd.toDF(["File_Path", "Size", "Last_Modified", "File_Format"])

    # Show the collected metadata
    metadata_df.show()

    # Perform intelligent sampling on the collected metadata
    sampled_data_df = intelligent_sampling(metadata_df)
    for row in metadata_df.collect():
        file_name = row[0] 
        file_format = row[3]
        if file_format in ['csv','txt','parquet']:
            df = process_file(bucket=bucket_name,file_format=file_format, file_name=file_name)
            sampled_df = intelligent_sampling(df)
            file_name = file_name.split('.')[0]
            write_to_s3(sampled_df=sampled_df,file_format=file_format,output_bucket=output_bucket,file_name="sampled_"+file_name)
            sampled_df.show(5)

# Show the sampled data
# sampled_data_df.show()

# Commit the job
job.commit()

