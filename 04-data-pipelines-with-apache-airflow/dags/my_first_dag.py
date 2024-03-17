from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone

# dag_id, start_data, schedule are minimum require
with DAG(
    dag_id="my_first_dag",
    start_date=timezone.datetime(2024, 3, 10),
    schedule=None,
    catchup=False,
    tags=["DEB", "Skooldio"],
):

    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")

    t1 >> t2