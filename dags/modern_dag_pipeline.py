from datetime import datetime
from random import randint

from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator


@dag("modern_task_dag", start_date=datetime(2026, 2, 14), schedule="@hourly", catchup=False)
def model_pipeline():
    EmptyOperator(task_id="test_empty_operator")

    @task.python
    def model_operator_one():
        return randint(1, 20)

    @task.python
    def model_operator_two():
        return randint(1, 20)

    @task.python
    def model_operator_three():
        return randint(1, 20)

    @task.python
    def high_grade(value):
        print(f"The max value is {value}")

    @task.python
    def low_grade(value):
        print(f"The min value is {value}")

    @task.branch
    def branch_operator(data):

        print(f"xcom log {data}")
        max_value = max(data)

        if max_value > 10:
            return "high_grade"

        return "low_grade"

    media_a = model_operator_one()
    media_b = model_operator_two()
    media_c = model_operator_three()

    upstream = [media_a, media_b, media_c]

    batch_op = branch_operator(upstream)

    grades = [high_grade(batch_op), low_grade(batch_op)]


model_pipeline()
