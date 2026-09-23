with source as (
    select
        date,
        latitude,
        longitude,
        temp_max,
        temp_min,
        (temp_max + temp_min) / 2 as temp_avg,
        precipitation,
        weather_code
    from DATA226_DB.raw.weather_daily
    where date is not null
)

select * from source