import json
import os
from utility import (
    load_data_into_gcs,
    load_data_into_gcs_with_partition,
    load_data_into_bq,
    load_data_into_bq_with_partition
    )
    

from dotenv import load_dotenv
from google.cloud import bigquery, storage
from google.oauth2 import service_account

load_dotenv()

DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
location = "asia-southeast1"

# Prepare and Load Credentials to Connect to GCP Services
# keyfile_gcs = "YOUR_KEYFILE_PATH_FOR_GCS"
keyfile_gcs = os.environ.get("GCS_KEYFILE_PATH")
service_account_info_gcs = json.load(open(keyfile_gcs))
credentials_gcs = service_account.Credentials.from_service_account_info(
    service_account_info_gcs
)

keyfile_bigquery = os.environ.get("BigQ_KEYFILE_PATH")
service_account_info_bigquery = json.load(open(keyfile_bigquery))
credentials_bigquery = service_account.Credentials.from_service_account_info(
    service_account_info_bigquery
)

# "YOUR_PROJECT_ID"
project_id = os.environ.get("PROJECT_ID")

# Load data from Local to GCS
bucket_name = os.environ.get("BUCKET_NAME") 
storage_client = storage.Client(
    project=project_id,
    credentials=credentials_gcs,
)
bucket = storage_client.bucket(bucket_name)

# YOUR CODE HERE TO LOAD DATA TO GCS

files_list = ['addresses', 'order_items','products','promos']
partition_files_dict = {'events':'2021-02-10', 'orders':"2021-02-10", 'users':"2020-10-23"}

# GCS part
# not partition part
# load_data_into_gcs(DATA_FOLDER, bucket, files_list, BUSINESS_DOMAIN)

# partition part
# load_data_into_gcs_with_partition(DATA_FOLDER, bucket, partition_files_dict, BUSINESS_DOMAIN)


# Big Querry part
# not partition part

bigquery_client = bigquery.Client(
        project=project_id,
        credentials=credentials_bigquery,
        location=location,
    )

job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

load_data_into_bq(files_list, bigquery_client, bucket_name, job_config, location, BUSINESS_DOMAIN, project_id)


# # partition part
# bigquery_client = bigquery.Client(
#         project=project_id,
#         credentials=credentials_bigquery,
#         location=location,
#     )

# job_config = bigquery.LoadJobConfig(
#         skip_leading_rows=1,
#         write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#         source_format=bigquery.SourceFormat.CSV,
#         autodetect=True,
#         time_partitioning=bigquery.TimePartitioning(
#         type_=bigquery.TimePartitioningType.DAY,
#         field="created_at",
#         ),
#     )

# load_data_into_bq_with_partition(partition_files_dict, bigquery_client, bucket_name, job_config, location, BUSINESS_DOMAIN, project_id)



# for data, dt in partition_files_dict.items():
#     partition = dt.replace("-", "")
#     file_path = f"{DATA_FOLDER}/{data}.csv"
#     destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{dt}/{data}.csv"
#     # Load data from GCS to BigQuery
    
#     table_id = f"{project_id}.deb_bootcamp.{data}${partition}"
    
#     job = bigquery_client.load_table_from_uri(
#         f"gs://{bucket_name}/{destination_blob_name}",
#         table_id,
#         job_config=job_config,
#         location=location,
#     )
#     job.result()

#     table = bigquery_client.get_table(table_id)
#     print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")