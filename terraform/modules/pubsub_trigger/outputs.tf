# output "service_url" {
#   value = google_cloudfunctions2_function.trigger_service.url
# }

output "pubsub_id" {
  value = google_pubsub_topic.trigger_topic.id
}
