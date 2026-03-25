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
 > docker ps -a
 > docker exec -it <RedPanda Container ID> rpk topic delete ny-traffic-events
 > docker exec -it 777de7adc127 rpk topic delete ny-traffic-events


 # Airflow Setup
 > pwd
 /home/ani.bhai.yt2022/de-zc-finalproject/orchestration

 > mkdir dags
 > mkdir logs
 > mkdir terraform
 > cd terraform
 > mkdir keys

> docker compose up

airflow-init will fail due to permission issue in ./logs folder.
> ls
Dockerfile  dags  docker-compose.yaml  logs  plugins  requirements.txt  terraform

dags logs plugins terraform folders are created because they are mapped in docker-compose file.

> pwd
> /home/ani.bhai.yt2022/de-zc-finalproject/orchestration
> sudo chown -R 50000:0 ./logs 
> sudo chmod -R 775 ./logs

This will fix the permission issue and airflow will successfully run

Add Port 8085 to do PortForwarding in VS Code.
airflow web UI can be reached at localhost:8085

Airflow setup successfully.

Created ny_traffic_events_dag.py file
In Airflow create Variables
Admin => Variables => Click +
Key=bucket_name
Value=ny_traffic_events

Key=location
Value=us-south1

Key=ny_traffic_api_key
Value=

Key=project_id
Value=dtc-ab-de-2026

Key=svc_AccountEmail
Value=

Also we need to add Connections conn Id:
Go to Admin > Connections > Click + 
connection Id: google_cloud_default
Connection Type: Google Cloud
Project Id: dtc-ab-de-2026
Keyfile JSON: upload your GCP-CREDS.json

After adding Connection: Jobs is successfully submitted to google Dataflow Jobs
fetch_api_to_gcs successful
prepare_transform_script successful
start_gcs_to_bigquery job submitted to google Dataflow Jobs successfully but due to some issue in JSON data, job failed.
clean_duplicate_data successful


# ny_traffic_events_dag_kafka.py
fetch_and_send_to_kafka successful
kafka_to_gcs_dataflow Job Submitted Successfully but Error lunching in GCP.

Tried to use Internal IP/External IP in Redpanda docker-compose config. Still not able to solve the issue.
For Localhost: test_kafka_consumer_localhost.py
- PLAINTEXT://de-ab-project-redpanda:29092,OUTSIDE://localhost:9092
Stream Data Consumer works

For Internal IP: test_kafka_consumer_Internal_IP.py
- PLAINTEXT://de-ab-project-redpanda:29092,OUTSIDE://10.206.0.3:9092
Steam Data Consumer works

But Dataflow job doesn't work on above both config.
Did lot of Google AI search and followed instruction, didn't help.
Created Firewall Policy suggested by AI. but didn't help.

API Seems to be changed and Schema also got changed
https://511ny.org/api/v2/get/event

Adjusting Terraform