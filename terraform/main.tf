terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.19.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# GCS Data Lake Storage Bucket
resource "google_storage_bucket" "ny_traffic_events_bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 1  # Keep for 30 days
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# Create a directory for temporary files of Dataflow
resource "google_storage_bucket_object" "dataflow_temp_folder" {
  name    = "temp/"
  content = "temp folder for dataflow"
  bucket  = google_storage_bucket.ny_traffic_events_bucket.name
}

# Create a directory for Dataflow staging files
resource "google_storage_bucket_object" "dataflow_staging_folder" {
  name    = "staging/"
  content = "staging folder for dataflow"
  bucket  = google_storage_bucket.ny_traffic_events_bucket.name
}

# BigQuery Dataset - Raw Data
resource "google_bigquery_dataset" "raw_dataset" {
  dataset_id = var.raw_dataset_name
  location   = var.location
}

# BigQuery Dataset - Processed Data
resource "google_bigquery_dataset" "processed_dataset" {
  dataset_id = var.processed_dataset_name
  location   = var.location
}

# BigQuery raw data table
resource "google_bigquery_table" "ny_traffic_events_table" {
  dataset_id = google_bigquery_dataset.raw_dataset.dataset_id
  table_id   = "ny_traffic_events"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingestion_time"
  }

  schema = <<EOF
[
  {
    "name": "ID",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "RegionName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "CountyName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "Severity",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "RoadwayName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DirectionOfTravel",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "Description",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "Location",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "LanesAffected",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "PrimaryLocation",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "SecondaryLocation",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "FirstArticleCity",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "SecondCity",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "EventType",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "EventSubType",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "LastUpdated",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "Latitude",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "Longitude",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "PlannedEndDate",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "Reported",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "StartDate",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "ingestion_time",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "processing_time",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  }
]
EOF
}

# Load Dataflow job script into GCS
resource "google_storage_bucket_object" "dataflow_job_file" {
  name   = "jobs/kafka_to_gcs_pipeline.py"
  bucket = google_storage_bucket.ny_traffic_events_bucket.name
  source = "../src/dataflow/kafka_to_gcs_pipeline.py"
}