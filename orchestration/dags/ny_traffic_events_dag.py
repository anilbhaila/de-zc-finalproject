from datetime import datetime, timedelta
import os
import json
import logging
import requests
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.cloud import storage
from airflow.models import Variable


# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,  # Increased retries
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
@dag(
    default_args=default_args,
    description='NY Traffic Events Pipeline',
    schedule_interval=timedelta(minutes=10),  # Runs every 10 minutes
    start_date=datetime(2025, 3, 28),
    catchup=False,
    tags=['Real-Time','NY', 'Traffic', 'Events'],
)
def ny_traffic_events_pipeline():
    """
    NY Traffic Events ETL Pipeline:
    1. Fetch data from API and write to GCS (data lake)
    2. Use Dataflow to load GCS data to BigQuery (data warehouse)
    3. Clean duplicate data in BigQuery
    """
    
    # Step 1: Fetch data from API and write to GCS
    @task(task_id="fetch_api_to_gcs")
    def fetch_api_to_gcs(**kwargs):
        """Fetch NY traffic events data from 511ny API and write to GCS"""
        import requests
        from google.cloud import storage
        import json
        from datetime import datetime
        import uuid
        
        # API key
        api_key = Variable.get("ny_traffic_api_key")
        
        # API parameters
        base_url = 'https://511ny.org/api/v2/get/'
        endpoint = "event"
        query_string = f"?key={api_key}&format=json"
        url = base_url + endpoint + query_string
        
        try:
            # Send API request
            response = requests.get(url)
            response.raise_for_status()
            
            # Parse response
            ny_traffic_events = response.json()
            
            # Add timestamp and process coordinates
            timestamp = datetime.now().isoformat()
            for event in ny_traffic_events:
                # Add timestamp
                clean_data = {}
                clean_data['ID'] = event.get('ID', str(uuid.uuid4()))
                clean_data['SourceId'] = event.get('SourceId', 'Unknown')
                clean_data['Organization'] = event.get('Organization', 'Unknown')
                clean_data['RoadwayName'] = event.get('RoadwayName', 'Unknown')
                clean_data['DirectionOfTravel'] = event.get('DirectionOfTravel', 'Unknown')
                clean_data['Description'] = event.get('Description', 'Unknown')
                clean_data['DirectionOfTravel'] = event.get('DirectionOfTravel', 'Unknown')
                clean_data['Reported'] = event.get('Reported', 'Unknown')
                unix_time = event.get('LastUpdated', None)
                if unix_time is not None:
                    timestamp = datetime.fromtimestamp(unix_time).isoformat()    

                clean_data['LastUpdated'] = timestamp
                clean_data['StartDate'] = event.get('StartDate', 'Unknown')
                clean_data['PlannedEndDate'] = event.get('PlannedEndDate', 'Unknown')
                clean_data['LanesAffected'] = event.get('LanesAffected', 'Unknown')
                clean_data['Latitude'] = event.get('Latitude', 'Unknown')
                clean_data['Longitude'] = event.get('Longitude', 'Unknown')
                clean_data['LatitudeSecondary'] = event.get('LatitudeSecondary', 'Unknown')
                clean_data['LongitudeSecondary'] = event.get('LongitudeSecondary', 'Unknown')
                clean_data['EventType'] = event.get('EventType', 'Unknown')
                clean_data['EventSubType'] = event.get('EventSubType', 'Unknown')
                clean_data['IsFullClosure'] = event.get('IsFullClosure', 'Unknown')
                clean_data['Severity'] = event.get('Severity', 'Unknown')
                clean_data['Comment'] = event.get('Comment', 'Unknown')
                clean_data['EncodedPolyline'] = event.get('EncodedPolyline', 'Unknown')
                clean_data['Recurrence'] = event.get('Recurrence', 'Unknown')
                clean_data['County'] = event.get('County', 'Unknown')
                clean_data['State'] = event.get('State', 'Unknown')
                clean_data['processing_time'] = event.get('processing_time', None)
                clean_data['ingestion_time'] = timestamp
                
                event.clear()
                event.update(clean_data)

            # Get current time for filename - use UTC time for consistency
            current_time = datetime.now().strftime('%Y%m%d-%H%M%S')
            unique_id = str(uuid.uuid4())[:8]
            day_folder = datetime.now().strftime('%Y%m%d')
            filename = f"ny_traffic_events-{current_time}-{unique_id}.json"
            
            # Connect to GCS and store data - organize data with date folders
            bucket_name = Variable.get("bucket_name")

            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(f"event-data/{day_folder}/{filename}")
            
            # Write JSON formatted data, one record per line
            json_lines = '\n'.join([json.dumps(record) for record in ny_traffic_events])
            blob.upload_from_string(json_lines)
            
            logging.info(f"Successfully fetched and wrote {len(ny_traffic_events)} traffic event records to GCS: gs://{bucket_name}/event-data/{day_folder}/{filename}")
            
            # Return current date folder for next task
            return day_folder
            
        except Exception as e:
            logging.error(f"API request or save to GCS failed: {str(e)}")
            raise
    
    # Step 2: Prepare JavaScript transform file
    @task(task_id="prepare_transform_script")
    def prepare_transform_script(**kwargs):
        """Prepare JavaScript transform script on GCS"""
        from google.cloud import storage
        
        # Define transform script content
        transform_script = """
        function transform(line) {
          // Parse JSON
          var eventData = JSON.parse(line);
          
          // Keep original fields
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
                {"name": "ID", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "SourceId", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Organization", "type": "STRING", "mode": "NULLABLE"},
                {"name": "RoadwayName", "type": "STRING", "mode": "NULLABLE"},
                {"name": "DirectionOfTravel", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Description", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Reported", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "LastUpdated", "type": "TIMESTAMP", "mode": "REQUIRED"},
                {"name": "StartDate", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "PlannedEndDate", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "LanesAffected", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Latitude", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "Longitude", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "LatitudeSecondary", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "LongitudeSecondary", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "EventType", "type": "STRING", "mode": "NULLABLE"},
                {"name": "EventSubType", "type": "STRING", "mode": "NULLABLE"},
                {"name": "IsFullClosure", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "Severity", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Comment", "type": "STRING", "mode": "NULLABLE"},
                {"name": "EncodedPolyline", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Recurrence", "type": "STRING", "mode": "NULLABLE"},
                {"name": "County", "type": "STRING", "mode": "NULLABLE"},
                {"name": "State", "type": "STRING", "mode": "NULLABLE"},
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
    
    # Step 3: Use Dataflow template to load GCS data to BigQuery
    @task(task_id="start_gcs_to_bigquery", retries=5, retry_delay=timedelta(minutes=2))
    def start_gcs_to_bigquery(script_paths, day_folder, **kwargs):
        """Start Dataflow job to load from GCS to BigQuery"""
        from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
        
        # Changed location to North America region
        gcs_to_bigquery = DataflowStartFlexTemplateOperator(
            task_id="gcs_to_bigquery_dataflow",
            project_id=Variable.get("project_id"),
            location=Variable.get("location"),  
            wait_until_finished=False,  
            body={
                "launchParameter": {
                    "containerSpecGcsPath": f"gs://dataflow-templates-{Variable.get('location')}/latest/flex/GCS_Text_to_BigQuery_Flex",  # Updated template path
                    "jobName": f"gcs-to-bq-job-{day_folder.replace('-', '')}",
                    "parameters": {
                        "javascriptTextTransformFunctionName": "transform",
                        "javascriptTextTransformGcsPath": script_paths["transform_path"],
                        "JSONPath": script_paths["schema_path"],
                        "inputFilePattern": f"gs://{Variable.get('bucket_name')}/event-data/{day_folder}/*.json",  # Only process that day's data
                        "outputTable": f"{Variable.get('project_id')}:ny_traffic_events_raw.ny_traffic_events",
                        "bigQueryLoadingTemporaryDirectory": f"gs://{Variable.get('bucket_name')}/temp/",
                        "tempLocation": f"gs://{Variable.get('bucket_name')}/temp/",
                        "numWorkers": "1",  # Use minimum workers
                        "workerMachineType": "e2-standard-2"  # Use smaller machine type
                    },
                    "environment": {
                        "serviceAccountEmail": f"{Variable.get('svc_AccountEmail')}"
                    }
                }
            }
        )
        
        # Execute job
        return gcs_to_bigquery.execute(context=kwargs)
    
    # Step 4: Clean duplicate data in BigQuery
    @task(task_id="clean_duplicate_data", trigger_rule="all_done")
    def clean_duplicate_data(**kwargs):
        """Clean duplicate data in BigQuery"""
        # Fix deduplication query to maintain partitioning
        dedup_query = """
        -- Create a temporary table with distinct records
        CREATE OR REPLACE TEMP TABLE temp_deduped AS
        SELECT DISTINCT * FROM `dtc-ab-de-2026.ny_traffic_events_raw.ny_traffic_events`;

        -- Delete all records from the partitioned table
        DELETE FROM `dtc-ab-de-2026.ny_traffic_events_raw.ny_traffic_events` 
        WHERE TRUE;

        -- Insert the deduplicated records back
        INSERT INTO `dtc-ab-de-2026.ny_traffic_events_raw.ny_traffic_events`
        SELECT * FROM temp_deduped;
        """
        
        clean_task = BigQueryExecuteQueryOperator(
            task_id="clean_duplicates_in_bigquery",
            sql=dedup_query,
            use_legacy_sql=False,
            location=Variable.get("location")  # BigQuery location remains the same
        )
        
        return clean_task.execute(context=kwargs)
    
    # Define task dependencies
    day_folder = fetch_api_to_gcs()
    transform_files = prepare_transform_script()
    load_to_bq = start_gcs_to_bigquery(transform_files, day_folder)
    clean_duplicates = clean_duplicate_data()
    
    # Set task order
    day_folder >> transform_files >> load_to_bq >> clean_duplicates

# Instantiate DAG
ny_traffic_events_dag = ny_traffic_events_pipeline()