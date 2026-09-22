# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(latitude, longitude):
    """Get the past 60 days of weather Toronto"""

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,  # only past weather
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ],
        "timezone": "America/Toronto"
    }

    response = requests.get(url, params=params)
    return response.json()


@task
def transform(data):
    daily = data["daily"]
    records = []
    for i in range(len(daily["time"])):
        records.append({
            "date": daily["time"][i],
            "temp_max": daily["temperature_2m_max"][i],
            "temp_min": daily["temperature_2m_min"][i],
            "precipitation": daily["precipitation_sum"][i],
            "weather_code": daily["weather_code"][i]
        })
    return records

@task
def load(records, target_table, latitude, longitude):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                latitude NUMBER,
                longitude NUMBER,
                date DATE,
                temp_max FLOAT,
                temp_min FLOAT,
                precipitation FLOAT,
                weather_code INT,
                PRIMARY KEY (latitude, longitude, date)
            )
        """)
        cur.execute(f"DELETE FROM {target_table}")
        for r in records:
            sql = f"""
                INSERT INTO {target_table}
                (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
                VALUES ({latitude}, {longitude}, '{r['date']}',
                        {r['temp_max']}, {r['temp_min']}, {r['precipitation']},
                        {r['weather_code']})
            """
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'weather_etl',
    start_date = datetime(2026,9,21),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    target_table = "raw.weather_daily"
    latitude = float(Variable.get("LATITUDE"))
    longitude = float(Variable.get("LONGITUDE"))
    
    data = extract(latitude, longitude)
    records = transform(data)
    load(records, target_table, latitude, longitude)