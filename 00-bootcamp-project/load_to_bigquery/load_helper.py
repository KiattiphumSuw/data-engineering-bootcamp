from google.cloud import bigquery
from google.oauth2 import service_account

def load(DATA_FOLDER, project_id, client, job_config, files_list):
    for data in files_list:
        file_path = f"{DATA_FOLDER}/{data}.csv"
        with open(file_path, "rb") as f:
            table_id = f"{project_id}.deb_bootcamp.{data}"
            job = client.load_table_from_file(f, table_id, job_config=job_config)
            job.result()

        table = client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


def load_partition(DATA_FOLDER, project_id, client, job_config, partition_files_dict):
    for data, dt in partition_files_dict.items():
        partition = dt.replace("-", "")
        file_path = f"{DATA_FOLDER}/{data}.csv"
        with open(file_path, "rb") as f:
            table_id = f"{project_id}.deb_bootcamp.{data}${partition}"
            job = client.load_table_from_file(f, table_id, job_config=job_config)
            job.result()

        table = client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")