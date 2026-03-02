from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd


def upsert_csv_to_postgres():
    # 1. Load the CSV
    file_path = '/usr/local/airflow/dags/data/users_update.csv'
    df = pd.read_csv(file_path)

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()

    # 2. Load to STAGING table (This wipes 'stg_users' every time)
    df.to_sql('stg_users', con=engine, if_exists='replace', index=False)

    # 3. The Idempotent "Upsert" SQL
    # We assume 'user_id' is the PRIMARY KEY in 'production_users'
    upsert_sql = """
    INSERT INTO production_users (user_id, email, last_login)
    SELECT user_id, email, last_login FROM stg_users
    ON CONFLICT (user_id) 
    DO UPDATE SET 
        email = EXCLUDED.email,
        last_login = EXCLUDED.last_login;
    """

    # 4. Execute the merge
    pg_hook.run(upsert_sql)
    print("Upsert completed: Data synchronized without duplicates.")
