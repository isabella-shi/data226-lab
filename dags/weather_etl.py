# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
def extract(city, latitude, longitude):
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
        "timezone": "auto"
    }

    response = requests.get(url, params=params)
    return {"city": city, "data": response.json()}


@task
def transform(data):
    city = data["city"]
    daily = data["data"]["daily"]
    records = []
    for i in range(len(daily["time"])):
        records.append({
            "city": city,
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
                city VARCHAR,
                latitude NUMBER,
                longitude NUMBER,
                date DATE,
                temp_max FLOAT,
                temp_min FLOAT,
                precipitation FLOAT,
                weather_code INT,
                PRIMARY KEY (city, date)
            )
        """)
        city = records[0]["city"]
        cur.execute(f"DELETE FROM {target_table} WHERE city = %s", (city,))

        insert_sql = f"""
            INSERT INTO {target_table}
            (city, latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        for r in records:
            cur.execute(insert_sql, (
                r['city'],
                latitude,
                longitude,
                r['date'],
                r['temp_max'],
                r['temp_min'],
                r['precipitation'],
                r['weather_code'],
            ))
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

    cities = {
        "Toronto": (float(Variable.get("TORONTO_LATITUDE")), float(Variable.get("TORONTO_LONGITUDE"))),
        "Seoul": (float(Variable.get("SEOUL_LATITUDE")), float(Variable.get("SEOUL_LONGITUDE"))),
    }

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="dbt_weather_analytics",
    )

    load_tasks = []

    for city, (lat, lon) in cities.items():
        data = extract(city, lat, lon)
        records = transform(data)
        load_task = load(records, target_table, lat, lon)
        load_tasks.append(load_task)

    load_tasks >> trigger_dbt