{% snapshot snapshot_weather_metrics %}

{{
    config(
        target_database='DATA226_DB',
        target_schema='snapshot',
        unique_key="city || '_' || cast(date as varchar)",
        strategy='timestamp',
        updated_at='date'
    )
}}

select
    * exclude (date),
    date::timestamp_ntz as date
from {{ ref('weather_metrics') }}

{% endsnapshot %}