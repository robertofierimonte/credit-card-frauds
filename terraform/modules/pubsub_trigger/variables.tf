variable "project_id" {
  description = "The ID of the project where the cloud scheduler will be created."
  type        = string
}

variable "region" {
  description = "Region where the scheduler job resides."
  type        = string
}

variable "pubsub_topic_name" {
  description = "Name of the Pub/Sub topic to create for triggering pipelines."
  type        = string
}

variable "pubsub_subscr_ack_deadline" {
  description = "Duration (in seconds) of the maximum time after a subscriber receives a message before the subscriber should acknowledge the message."
  type        = number
}

variable "pubsub_service_account" {
  description = "Name of SA PubSub will use to invoke Cloud Run service."
  type        = string
}

variable "cloud_function_name" {
  description = "Name of the Cloud Functions Service to create for triggering pipelines."
  type        = string
}

variable "cloud_function_config" {
  description = "Map of configurations for cloud function trigger."
  type = object({
    service_account       = string
    environment_variables = map(string)
    archive_bucket        = string
    archive_object        = string
    runtime               = string
  })
}
