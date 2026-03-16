from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

t_env.get_config().set("parallelism.default", "1")

source_ddl = """
CREATE TABLE green_trips (
    lpep_pickup_datetime VARCHAR,
    lpep_dropoff_datetime VARCHAR,
    PULocationID INT,
    DOLocationID INT,
    passenger_count INT,
    trip_distance DOUBLE,
    tip_amount DOUBLE,
    total_amount DOUBLE,

    event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),

    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'green-trips',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'flink-green',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
"""

sink_ddl = """
CREATE TABLE q4_results (
    window_start TIMESTAMP,
    pulocationid INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, pulocationid) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'q4_results',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
"""

t_env.execute_sql(source_ddl)
t_env.execute_sql(sink_ddl)

query = """
INSERT INTO q4_results
SELECT
    window_start,
    PULocationID,
    COUNT(*) as num_trips
FROM TABLE(
    TUMBLE(
        TABLE green_trips,
        DESCRIPTOR(event_timestamp),
        INTERVAL '5' MINUTES
    )
)
GROUP BY window_start, PULocationID
"""

t_env.execute_sql(query).wait()