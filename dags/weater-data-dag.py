from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import requests


@dag(
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["weather", "taskflow"],
)
def weather_etl_pipeline():

    # Fetch data from weather api
    @task.python
    def extract_weather_data():
        url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&daily=temperature_2m_max&timezone=GMT"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()

    # Clean and format
    @task.python
    def transform_data(raw_data: dict):
        daily = raw_data['daily']
        return {
            "date": daily['time'][0],
            "max_temp": daily['temperature_2m_max'][0]
        }

    # Idempotent Load using PostgresHook
    @task
    def load_to_postgres(clean_data: dict):
        pg_hook = PostgresHook(postgres_conn_id="")

        # IDEMPOTENCY: Using "ON CONFLICT" ensures we don't double-count days
        upsert_sql = """
            INSERT INTO weather (report_date, max_temp)
            VALUES (%s, %s)
            ON CONFLICT (report_date) 
            DO UPDATE SET max_temp = EXCLUDED.max_temp;
        """

        pg_hook.run(upsert_sql, parameters=(
            clean_data['date'], clean_data['max_temp']))
        print(f"Successfully synced data for {clean_data['date']}")

    # Define dependencies
    weather_json = extract_weather_data()
    formatted_data = transform_data(weather_json)
    load_to_postgres(formatted_data)


# Instantiate DAG
weather_etl_pipeline()
