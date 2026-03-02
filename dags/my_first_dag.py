from random import randint
from datetime import datetime
from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator


class Model:
    model_a = "model_a"
    model_b = "model_b"
    model_c = "model_c"


model = Model()


task_ids = {"model_a": "model_a", "model_b": "model_b", "model_c": "model_c"}


def _get_model_accuracy():
    return randint(1, 20)


def reuseable_operator(task_id):
    return PythonOperator(
        task_id=task_id, python_callable=_get_model_accuracy
    )


def branch(ti):
    accuracies = ti.xcom_pull(task_ids=["model_a", "model_b", "model_c"])
    if accuracies > 8:
        return "accurate"
    return "inaccurate"


with DAG("My_unique_first_DAG", start_date=datetime(2026, 1, 1), schedule="@daily", catchup=False) as dag:

    operator_a = reuseable_operator(model.model_a)

    operator_b = reuseable_operator("model_b")

    operator_c = reuseable_operator("model_c")

    python_branch = BranchPythonOperator(
        task_id="python_branch", python_callable=branch)

    accurate = BashOperator(
        task_id="accurate_operator", bash_command="echo 'operator B'")

    inaccurate = BashOperator(
        task_id="inaccurate_operator", bash_command="echo 'operator B'")

    [operator_a, operator_b, operator_b] >> python_branch >> [inaccurate, accurate]
