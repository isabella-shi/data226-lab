WITH base AS (
    SELECT *
    FROM {{ ref('weather_daily') }}
),

with_dry_flag AS (
    SELECT *,
        IFF(precipitation >= 1, 0, 1) AS is_dry,
        SUM(IFF(precipitation >= 1, 1, 0))
            OVER (
                PARTITION BY city
                ORDER BY date
            ) AS wet_group
    FROM base
)

SELECT
    city,
    date,
    latitude,
    longitude,
    temp_max,
    temp_min,
    temp_avg,
    precipitation,

    -- 7-day moving average of temperature
    AVG(temp_avg) OVER (
	PARTITION BY city
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS temp_7d_moving_avg,

    -- Temperature anomaly: today's temp vs. the 7-day moving average
    temp_avg - AVG(temp_avg) OVER (
	PARTITION BY city
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS temp_anomaly,

    -- 7-day rolling rainfall total
    SUM(precipitation) OVER (
	PARTITION BY city
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_rainfall,

    -- Dry spell length: consecutive days with < 1mm precipitation
    SUM(is_dry) OVER (
        PARTITION BY city, wet_group
        ORDER BY date
    ) AS dry_spell_days

FROM with_dry_flag
ORDER BY city, date
