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
    field = "LastUpdated"
  }

  schema = <<EOF
[
  {
    "name": "ID",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "SourceId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "Organization",
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
    "name": "Reported",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "LastUpdated",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "StartDate",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "PlannedEndDate",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "LanesAffected",
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
    "name": "LatitudeSecondary",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "LongitudeSecondary",
    "type": "FLOAT",
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
    "name": "IsFullClosure",
    "type": "BOOL",
    "mode": "NULLABLE"
  },
  {
    "name": "Severity",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "Comment",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "EncodedPolyline",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "Recurrence",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "County",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "State",
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
