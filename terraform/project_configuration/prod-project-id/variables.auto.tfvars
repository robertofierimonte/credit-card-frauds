project_id  = "robertofierimonte-ml-pipe"
region      = "europe-west2"
name_suffix = "prod"

pubsub_topic_name          = "credit-card-frauds-retraining-topic"
pubsub_service_account     = "scheduler-sa@robertofierimonte-ml-pipe.iam.gserviceaccount.com"
pubsub_subscr_ack_deadline = 120
cloud_run_name             = "credit-card-frauds-retraining-trigger"

cloud_schedulers_config = {
  prediction = {
    name         = "credit-card-frauds-automated-retraining-scheduler",
    description  = "Trigger - Credit card frauds retraining pipeline in Vertex.",
    schedule     = "0 0 * * 2",
    time_zone    = "UTC",
    payload_file = "../src/pipelines/training/payloads/prod.json",
  },
}
