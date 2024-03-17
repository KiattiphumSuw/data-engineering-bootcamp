import os

def load_data_into_gcs(DATA_FOLDER, bucket, files_list, BUSINESS_DOMAIN):
    for data in files_list:

        destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"
        file_path = f"{DATA_FOLDER}/{data}.csv"

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)

def load_data_into_gcs_with_partition(DATA_FOLDER, bucket, partition_files_dict, BUSINESS_DOMAIN):
    for data, dt in partition_files_dict.items():
        partition = dt.replace("-", "")
        file_path = f"{DATA_FOLDER}/{data}.csv"
        destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{dt}/{data}.csv"

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)

def load_data_into_bq(files_list, bigquery_client, bucket_name, job_config, location, BUSINESS_DOMAIN,project_id):
    for data in files_list:
        destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"
        # Load data from GCS to BigQuery
        
        table_id = f"{project_id}.deb_bootcamp.{data}"
        
        job = bigquery_client.load_table_from_uri(
            f"gs://{bucket_name}/{destination_blob_name}",
            table_id,
            job_config=job_config,
            location=location,
        )
        job.result()

        table = bigquery_client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

def load_data_into_bq_with_partition(partition_files_dict, bigquery_client, bucket_name, job_config, location, BUSINESS_DOMAIN, project_id):
    for data, dt in partition_files_dict.items():
        partition = dt.replace("-", "")
        file_path = f"{DATA_FOLDER}/{data}.csv"
        destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{dt}/{data}.csv"
        # Load data from GCS to BigQuery
        
        table_id = f"{project_id}.deb_bootcamp.{data}${partition}"
        
        job = bigquery_client.load_table_from_uri(
            f"gs://{bucket_name}/{destination_blob_name}",
            table_id,
            job_config=job_config,
            location=location,
        )
        job.result()

        table = bigquery_client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")