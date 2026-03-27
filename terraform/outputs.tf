output "bucket_name" {
  value       = google_storage_bucket.ny_traffic_events_bucket.name
  description = "The GCS bucket name"
}

output "raw_dataset" {
  value       = google_bigquery_dataset.raw_dataset.dataset_id
  description = "BigQuery raw dataset ID"
}

output "processed_dataset" {
  value       = google_bigquery_dataset.processed_dataset.dataset_id
  description = "BigQuery processed dataset ID"
}


output "temp_location" {
  value       = "${google_storage_bucket.ny_traffic_events_bucket.name}/temp"
  description = "The GCS temp location for Dataflow jobs"
}

output "staging_location" {
  value       = "${google_storage_bucket.ny_traffic_events_bucket.name}/staging"
  description = "The GCS staging location for Dataflow jobs"
}