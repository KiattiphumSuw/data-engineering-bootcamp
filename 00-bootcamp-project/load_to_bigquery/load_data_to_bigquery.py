import json
import os
import load_helper as h

from dotenv import load_dotenv

from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()
DATA_FOLDER = "data"

# ตัวอย่างการกำหนด Path ของ Keyfile ในแบบที่ใช้ Environment Variable มาช่วย
# จะทำให้เราไม่ต้อง Hardcode Path ของไฟล์ไว้ในโค้ดของเรา

keyfile = os.environ.get("KEYFILE_PATH")
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = str(os.environ.get("PROJECT_ID"))

client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

# YOUR CODE HERE

# unpartition tables are order_items products promos
files_list = ['addresses', 'order_items','products','promos']

job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
)

h.load(DATA_FOLDER, project_id, client, job_config, files_list)

# partition tables are orders users
# In the formatting, file name : date
partition_files_dict = {'events':'2021-02-10', 'orders':"2021-02-10", 'users':"2020-10-23"}

job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at",
    ),
)

h.load_partition(DATA_FOLDER, project_id, client, job_config, partition_files_dict)