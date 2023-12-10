# Pub/Sub topic + Cloud Run Trigger
module "pubsub_trigger" {
  source                     = "./modules/pubsub_trigger"
  project_id                 = var.project_id
  region                     = var.region
  pubsub_topic_name          = "${var.pubsub_topic_name}-${var.name_suffix}"
  cloud_run_name             = "${var.cloud_run_name}-${var.name_suffix}"
  pubsub_service_account     = var.pubsub_service_account
  pubsub_subscr_ack_deadline = var.pubsub_subscr_ack_deadline
  cloud_run_config           = var.cloud_run_config
}

# Cloud Scheduler (for triggering pipelines)
module "pubsub_scheduler" {
  for_each       = var.cloud_schedulers_config
  source         = "./modules/pubsub_scheduler"
  project_id     = var.project_id
  region         = var.region
  scheduler_name = "${each.value.name}-${var.name_suffix}"
  description    = lookup(each.value, "description", null)
  schedule       = each.value.schedule
  time_zone      = lookup(each.value, "time_zone", "UTC")
  topic_id       = module.pubsub_trigger.pubsub_id
  attributes     = jsondecode(file(each.value.payload_file)).attributes
  data           = base64encode(jsonencode(jsondecode(file(each.value.payload_file)).data))

  depends_on = [
    module.pubsub_trigger,
  ]
}
