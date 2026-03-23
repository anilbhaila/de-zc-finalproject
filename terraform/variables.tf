variable "credentials" {
    description = "GCP Credentials"
    # Replace with your credential path
    default = ".keys/gcp-creds.json"
}

variable "project" {
    description = "GCP Project ID"
    # Replace with your project ID
    default = "dtc-ab-de-2026"
}

variable "region" {
    description = "GCP Region"
    # Replace with your location
    default = "us-south1"
}

variable "location" {
    description = "Data Location"
    default = "us-south1"
}

variable "raw_dataset_name" {
  description = "BigQuery Raw Dataset Name"
  default     = "ny_traffic_events_raw"
}

variable "processed_dataset_name" {
  description = "BigQuery Processed Dataset Name"
  default     = "ny_traffic_events_processed"
}

variable "gcs_bucket_name" {
    description = "Data Lake Bucket Name"
    # bucket name
    default = "ny_traffic_events"
}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"
}

variable "dataproc_cluster_name" {
  description = "Dataproc Cluster Name"
  default     = "ny_traffic_events-flink-cluster"
}

variable "dataproc_machine_type" {
  description = "Machine type for Dataproc cluster nodes"
  default     = "n1-standard-2"
}

variable "dataproc_image_version" {
  description = "Dataproc image version"
  default     = "2.1-debian10"
}