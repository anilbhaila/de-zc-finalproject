
from datetime import datetime, timedelta
import os
import json
import logging
import uuid
import requests
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from google.cloud import storage
from airflow.models import Variable


# Default parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
@dag(
    default_args=default_args,
    description='NY Traffic Events kafka Pipeline',
    schedule_interval=timedelta(minutes=10),  # Run once every 10 minutes
    start_date=datetime(2025, 3, 27),
    catchup=False,
    tags=['Real-Time','NY', 'Traffic', 'Events'],
)
def ny_traffic_events_kafka_pipeline():
    """
    NY Traffic Events ETL Pipeline:

    1. Fetch data from API and send it to Kafka

    2. Use Dataflow to write Kafka data to GCS (data lake)

    3. Use Dataflow to load GCS data into BigQuery (data warehouse)
    """
    
    # Step 1: Obtain data from the 511ny API and send it to Kafka
    @task(task_id="fetch_and_send_to_kafka")
    def fetch_and_send_to_kafka(**kwargs):
        """Retrieve NY traffic events data from the 511ny API and send it to Kafka."""
        import requests
        from kafka import KafkaProducer
        
        # API key
        api_key = Variable.get("ny_traffic_api_key")
        
        # API parameters
        base_url = 'https://511ny.org/api/'
        endpoint = "getevents"
        query_string = f"?key={api_key}&format=json"
        url = base_url + endpoint + query_string
       
        try:
            # Send API request
            response = requests.get(url)
            response.raise_for_status()
            
            # Parse response
            ny_traffic_events = response.json()
            
            # Add timestamp and filter data
            timestamp = datetime.now().isoformat()
            for event in ny_traffic_events:
                clean_data = {}
                clean_data['ID'] = event.get('ID', str(uuid.uuid4()))
                clean_data['ingestion_time'] = timestamp
                clean_data['RegionName'] = event.get('RegionName', 'Unknown')
                clean_data['CountyName'] = event.get('CountyName', 'Unknown')
                clean_data['Severity'] = event.get('Severity', 'Unknown')
                clean_data['RoadwayName'] = event.get('RoadwayName', 'Unknown')
                clean_data['DirectionOfTravel'] = event.get('DirectionOfTravel', 'Unknown')
                clean_data['Description'] = event.get('Description', 'Unknown')
                clean_data['Location'] = event.get('Location', 'Unknown')
                clean_data['LanesAffected'] = event.get('LanesAffected', 'Unknown')
                clean_data['PrimaryLocation'] = event.get('PrimaryLocation', 'Unknown')
                clean_data['SecondaryLocation'] = event.get('SecondaryLocation', 'Unknown')
                clean_data['FirstArticleCity'] = event.get('FirstArticleCity', 'Unknown')
                clean_data['SecondCity'] = event.get('SecondCity', 'Unknown')
                clean_data['EventType'] = event.get('EventType', 'Unknown')
                clean_data['EventSubType'] = event.get('EventSubType', 'Unknown')
                clean_data['LastUpdated'] = event.get('LastUpdated', 'Unknown')
                clean_data['Latitude'] = event.get('Latitude', 'Unknown')
                clean_data['Longitude'] = event.get('Longitude', 'Unknown')
                clean_data['PlannedEndDate'] = event.get('PlannedEndDate', 'Unknown')
                clean_data['Reported'] = event.get('Reported', 'Unknown')
                clean_data['StartDate'] = event.get('StartDate', 'Unknown')
                clean_data['processing_time'] = event.get('processing_time', None)
                event.clear()
                event.update(clean_data)

            # Connect to Kafka and send data
            kafka_server = Variable.get("kafka_bootstrap_servers", default_var="de-ab-project-redpanda:29092")
            logging.info(f"Connecting to Kafka server at: {kafka_server}")


            producer = KafkaProducer(
                bootstrap_servers=[kafka_server],  # Use your Kafka service
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            
            # Send each record
            for record in ny_traffic_events:
                producer.send('ny-traffic-events', value=record)
            
            # Ensure that all messages have been sent
            producer.flush()
            producer.close()
            
            logging.info(f"Sent {len(ny_traffic_events)} Events to Kafka")
            return len(ny_traffic_events)
            
        except Exception as e:
            logging.error(f"API Failure: {str(e)}")
            raise
    
    # Step 2: Use the Dataflow template to write Kafka data to GCS
    kafka_to_gcs = DataflowStartFlexTemplateOperator(
        task_id="kafka_to_gcs_dataflow",
        project_id=Variable.get("project_id"),
        location=Variable.get("location"),
          
        wait_until_finished=True,
        body={
            "launchParameter": {
                "containerSpecGcsPath": f"gs://dataflow-templates-us-south1/latest/flex/Kafka_to_Gcs_Flex",
                "jobName": "kafka-to-gcs-job",
                "parameters": {
                    "readBootstrapServerAndTopic": f"10.206.0.3:9092,ny-traffic-events",
                    "outputDirectory": f"gs://ny_traffic_events/event-data/",
                    "outputFilenamePrefix": "event-",
                    "windowDuration": "5m",
                    "kafkaReadAuthenticationMode": "NONE",
                    "messageFormat": "JSON",
                    "useBigQueryDLQ": "false",
                    "tempLocation": f"gs://ny_traffic_events/temp/"
                },

                "environment": {
                    "network": "default",
                    "subnetwork": "regions/us-south1/subnetworks/default",
                    "workerZone": "us-south1-b",
                    "serviceAccountEmail": "svc-dtc-ab-de-2026@dtc-ab-de-2026.iam.gserviceaccount.com"
                }
            }
        }
    )
    
    # Step 3: Create a task for the JavaScript transformation file
    @task(task_id="prepare_transform_script")
    def prepare_transform_script(**kwargs):
        """Prepare the JavaScript conversion script on the GCS"""
        from google.cloud import storage
        
        # Define the content of the conversion script
        transform_script = """
        function transform(line) {
          // Analysis JSON
          var eventData = JSON.parse(line);
          
          // Maintain the original field
          return JSON.stringify(eventData);
        }
        """
        
        # Upload to GCS
        bucket_name = Variable.get("bucket_name")
        
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob("scripts/transform.js")
        blob.upload_from_string(transform_script)
        
        # Create schema file
        schema_json = {
            "BigQuery Schema": [
                {"name": "ID", "type": "STRING", "mode": "REQUIRED"},
                {"name": "RegionName", "type": "STRING", "mode": "NULLABLE"},
                {"name": "CountyName", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Severity", "type": "STRING", "mode": "NULLABLE"},
                {"name": "RoadwayName", "type": "STRING", "mode": "NULLABLE"},
                {"name": "DirectionOfTravel", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Description", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Location", "type": "STRING", "mode": "NULLABLE"},
                {"name": "LanesAffected", "type": "STRING", "mode": "NULLABLE"},
                {"name": "PrimaryLocation", "type": "STRING", "mode": "NULLABLE"},
                {"name": "SecondaryLocation", "type": "STRING", "mode": "NULLABLE"},
                {"name": "FirstArticleCity", "type": "STRING", "mode": "NULLABLE"},
                {"name": "SecondCity", "type": "STRING", "mode": "NULLABLE"},
                {"name": "EventType", "type": "STRING", "mode": "NULLABLE"},
                {"name": "EventSubType", "type": "STRING", "mode": "NULLABLE"},
                {"name": "LastUpdated", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Latitude", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "Longitude", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "PlannedEndDate", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Reported", "type": "STRING", "mode": "NULLABLE"},
                {"name": "StartDate", "type": "STRING", "mode": "NULLABLE"},
                {"name": "ingestion_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
                {"name": "processing_time", "type": "TIMESTAMP", "mode": "NULLABLE"}
            ]
        }
        
        # Upload schema file
        schema_blob = bucket.blob("schemas/ny_traffic_events_schema.json")
        schema_blob.upload_from_string(json.dumps(schema_json))
        
        return {
            "transform_path": f"gs://{bucket_name}/scripts/transform.js",
            "schema_path": f"gs://{bucket_name}/schemas/ny_traffic_events_schema.json"
        }
    
    # Step 4: Use the Dataflow template to load GCS data into BigQuery
    @task(task_id="start_gcs_to_bigquery")
    def start_gcs_to_bigquery(script_paths, **kwargs):
        """Start the Dataflow job to load from GCS to BigQuery"""
        from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
        
        gcs_to_bigquery = DataflowStartFlexTemplateOperator(
            task_id="gcs_to_bigquery_dataflow",
            project_id=Variable.get("project_id"),
            location=Variable.get("location"),  
            wait_until_finished=False,  # Do not wait for completion, avoid resource issues blocking Airflow
            body={
                "launchParameter": {
                    "containerSpecGcsPath": f"gs://dataflow-templates-{Variable.get('location')}/latest/flex/GCS_Text_to_BigQuery_Flex",
                    "jobName": "gcs-to-bq-job",
                    "parameters": {
                        "javascriptTextTransformFunctionName": "transform",
                        "javascriptTextTransformGcsPath": script_paths["transform_path"],
                        "JSONPath": script_paths["schema_path"],
                        "inputFilePattern": f"gs://{Variable.get('bucket_name')}/event-data/event-*.json",
                        "outputTable": f"{Variable.get('project_id')}:ny_traffic_events_raw.ny_traffic_events",
                        "bigQueryLoadingTemporaryDirectory": f"gs://{Variable.get('bucket_name')}/temp/",
                        "tempLocation": f"gs://{Variable.get('bucket_name')}/temp/"
                    }
                },
                "environment": {
                    "serviceAccountEmail": f"{Variable.get('svc_AccountEmail')}"
                }
            }
        )
        
        # Execute task
        return gcs_to_bigquery.execute(context=kwargs)
    
    # Define task dependencies
    fetch_kafka = fetch_and_send_to_kafka()
    transform_files = prepare_transform_script()
    
    # Set task sequence
    fetch_kafka >> kafka_to_gcs >> transform_files >> start_gcs_to_bigquery(transform_files)

# Instantiate DAG
ny_traffic_events_kafka_pipeline_dag = ny_traffic_events_kafka_pipeline()
