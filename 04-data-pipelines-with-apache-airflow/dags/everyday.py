from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def _world():
    return "World"


with DAG(
    dag_id="everyday",
    schedule="0 0 * * *",
    start_date=timezone.datetime(2024, 3, 10),
    catchup=False, # this means catchup all data before timestamp
    tags=["DEB", "Skooldio"],
):

    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Hello'",
    )

    world = PythonOperator(
        task_id="world",
        python_callable=_world,
    )

    hello >> world