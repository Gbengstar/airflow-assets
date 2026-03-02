# from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import create_engine, text


def upsert_csv_to_postgres():
    print("Started running script")
    # 1. Load the CSV
    file_path = './payment.csv'
    df = pd.read_csv(file_path)
    df.rename(columns=str.lower, inplace=True)

    df["amt"] = pd.to_numeric(df["amount it"].str.replace(",", "", regex=True))

    database_url = "postgresql://gbengstar:admin@localhost:5432/postgres"

    engine = create_engine(database_url, echo=True)
    staging_table = "staging"
    prod_table = "production"

    create_prod_database = F"""
    CREATE TABLE IF NOT EXISTS {prod_table} (
    currency CHAR(3) NOT NULL,
    createdat TIMESTAMP,
    updatedat TIMESTAMP,
    status VARCHAR(20),
    metadata TEXT,
    reference VARCHAR(60) PRIMARY KEY,
    channel VARCHAR(20),
    userid VARCHAR(30),
    retries INTEGER,
    amt INTEGER
    )
    """

    connection = engine.connect()
    connection.execute(text(create_prod_database))

    connection.commit()
    # 2. Load to STAGING table (This wipes 'stg_users' every time)
    df.to_sql(staging_table, con=engine, if_exists='replace', index=False,)

    # 3. The Idempotent "Upsert" SQL
    # We assume 'user_id' is the PRIMARY KEY in 'production_users'
    table_col = "currency, createdat, updatedat, status, metadata, reference, channel, userid, retries, amt"

    upsert_sql = f"""
    INSERT INTO {prod_table} (
    {table_col}
    )
    SELECT  {table_col} FROM {staging_table}
    ON CONFLICT (reference)
    DO UPDATE SET
        currency = EXCLUDED.currency,
        createdat = EXCLUDED.createdat,
        updatedat = EXCLUDED.updatedat,
        status = EXCLUDED.status,
        metadata = EXCLUDED.metadata,
        reference = EXCLUDED.reference,
        userid = EXCLUDED.userid,
        retries = EXCLUDED.retries,
        amt = EXCLUDED.amt;
    """

    connection.execute(text(upsert_sql))
    print("Upsert completed: Data synchronized without duplicates.")
    connection.commit()


upsert_csv_to_postgres()
