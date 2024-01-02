# Cloud Storage Archive (for Cloud Function source code)
resource "google_storage_bucket_object" "source_code_archive" {
  source       = data.archive_file.source_code.output_path
  content_type = "application/zip"
  name         = "${join("/", slice(split("/", substr(var.cloud_function_config.archive_bucket, 5, -1)), 1, length(split("/", substr(var.cloud_function_config.archive_bucket, 5, -1)))))}/${var.cloud_function_config.archive_object}"
  bucket       = split("/", substr(var.cloud_function_config.archive_bucket, 5, -1))[0]
  depends_on   = [
    data.archive_file.source_code
  ]
}

# Cloud Function Trigger (for triggering pipelines)
resource "google_cloudfunctions2_function" "trigger_service" {
  name                  = var.cloud_function_name
  project               = var.project_id
  location              = var.region
  build_config {
    runtime     = var.cloud_function_config.runtime
    entry_point = "cf_handler"
    source {
      storage_source {
        bucket = split("/", substr(var.cloud_function_config.archive_bucket, 5, -1))[0]
        object = google_storage_bucket_object.source_code_archive.name
      }
    }
  }
  service_config {
    service_account_email = var.cloud_function_config.service_account
    environment_variables = var.cloud_function_config.environment_variables
  }
  event_trigger {
    trigger_region        = var.region
    event_type            = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic          = google_pubsub_topic.trigger_topic.id
    service_account_email = var.cloud_function_config.service_account
    retry_policy          = "RETRY_POLICY_RETRY"
  }
  lifecycle {
    replace_triggered_by = [
      google_storage_bucket_object.source_code_archive.md5hash
    ]
  }
  depends_on = [
    google_storage_bucket_object.source_code_archive
  ]
}

# Pub/Sub topic (for triggering pipelines)
resource "google_pubsub_topic" "trigger_topic" {
  project = var.project_id
  name    = var.pubsub_topic_name
}
