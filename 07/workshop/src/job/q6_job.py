from pyflink.table import EnvironmentSettings, TableEnvironment

# create streaming environment
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# keep things simple
t_env.get_config().set("parallelism.default", "1")

# Kafka source table
source_ddl = """
CREATE TABLE green_trips (
    lpep_pickup_datetime STRING,
    lpep_dropoff_datetime STRING,
    PULocationID INT,
    DOLocationID INT,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    tip_amount DOUBLE,
    total_amount DOUBLE,

    event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),

    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'green-trips',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'flink-green-q6',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
"""

t_env.execute_sql(source_ddl)

# tumbling window query
query = """
SELECT window_start, total_tips
FROM (
    SELECT
        window_start,
        SUM(tip_amount) AS total_tips
    FROM TABLE(
        TUMBLE(
            TABLE green_trips,
            DESCRIPTOR(event_timestamp),
            INTERVAL '1' HOUR
        )
    )
    GROUP BY window_start
)
ORDER BY total_tips DESC
LIMIT 1
"""

result = t_env.execute_sql(query)
result.print()