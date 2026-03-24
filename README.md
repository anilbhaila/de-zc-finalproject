# de-zc-finalproject
RealTime Data analysis

Command Run
> uv init --python=3.13
> uv add --dev jupyter
> uv add pandas

Run jupyter notebook
> uv run jupyter notebook

jupyter notebook can be reached in below url
http://localhost:8888/tree?token=c3efa89deefb67ebde3190669adb6f024d1dda4c4a7636c9
        
This url can be used to set jupyter kernel in VS Code, so that we can run notebook from within VS Code.
> touch notebook.ipynb

Tested the notebook.ipynb cell by trying to execute print(123) python code.
This will ask to setup jupyter kernel.

> touch docker-compose.yaml

Added DE ZoomCamp's Kestra docker-compose code 
> docker compose up -d


Followed linux.md instruction to install Apache Spark in Google VM

> uv add pyspark

Ran a test code in notebook cell to test pyspark is installed successfully

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

print(f"Spark version: {spark.version}")

df = spark.range(10)
df.show()

spark.stop()

Output:
WARNING: Using incubator modules: jdk.incubator.vector
Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
26/03/19 22:33:22 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark version: 4.1.1
                                                                                
+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
|  5|
|  6|
|  7|
|  8|
|  9|
+---+

kestra: http://localhost:8080
pgAdmin: http://localhost:8085

Python producer/consumer connects at Kafka protocol 
http://localhost:9092

Flink dashboard: http://localhost:8081

> docker-compose ps
lists all process started by docker-compose


To submit Job to Flink Job Manager

> uv run python src/producers/producer.py

> docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/pass_through_job.py \
    --pyFiles /opt/src -d


CREATE TABLE processed_events (
    PULocationID INTEGER,
    DOLocationID INTEGER,
    trip_distance DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    pickup_datetime TIMESTAMP
);


> uv run python src/consumers/consumer_postgres.py

SELECT count(*) FROM processed_events;

Output:
 count
-------
  1000


CREATE TABLE processed_events_aggregated (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, PULocationID)
);

> docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/aggregation_job.py \
    --pyFiles /opt/src -d

> uv run python src/producers/producer.py

SELECT window_start, count(*) as locations, sum(num_trips) as total_trips,
       round(sum(total_revenue)::numeric, 2) as revenue
FROM processed_events_aggregated
GROUP BY window_start
ORDER BY window_start;

Output:

     window_start     | locations | total_trips | revenue
----------------------+-----------+-------------+---------
 2025-11-01 00:00:00  |        ...
 2025-11-01 01:00:00  |        ...
 ...


 > uv run python src/producers/producer_realtime.py

 > watch -n 1 'PGPASSWORD=root docker compose exec pgdatabase psql -U root -d ny_traffic -c "SELECT window_start, sum(num_trips) as trips, round(sum(total_revenue)::numeric, 2) as revenue FROM processed_events_aggregated GROUP BY window_start ORDER BY window_start;"'

 
 Clean up
 > docker compose down

 To also remove the PostgreSQL data volume:
 > docker compose down -v

 Finally all these commands are working.


 Command:
 > docker exec -it <RedPanda Container ID> rpk topic delete ny-traffic-events
 > docker exec -it 777de7adc127 rpk topic delete ny-traffic-events