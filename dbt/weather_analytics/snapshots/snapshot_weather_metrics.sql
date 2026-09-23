{% snapshot snapshot_weather_metrics %}

{{
    config(
        target_database='DATA226_DB',
        target_schema='snapshot',
        unique_key='date',
        strategy='timestamp',
        updated_at='date'
    )
}}

select * from {{ ref('weather_metrics') }}

{% endsnapshot %}
