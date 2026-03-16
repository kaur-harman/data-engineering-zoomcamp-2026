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
    'properties.group.id' = 'flink-green-q5',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
"""

sink_ddl = """
CREATE TABLE q5_results (
    pulocationid INT,
    num_trips BIGINT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'q5_results',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
"""

t_env.execute_sql(source_ddl)
t_env.execute_sql(sink_ddl)

query = """
INSERT INTO q5_results
SELECT
    PULocationID,
    COUNT(*) AS num_trips
FROM green_trips
GROUP BY
    PULocationID,
    SESSION(event_timestamp, INTERVAL '5' MINUTE)
"""

t_env.execute_sql(query).wait()