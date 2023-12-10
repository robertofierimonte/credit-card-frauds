# Cloud Run Trigger (for triggering pipelines)
resource "google_cloud_run_service" "run_service" {
  project  = var.project_id
  name     = var.cloud_run_name
  location = var.region

  template {
    spec {
      containers {
        image   = var.cloud_run_config.image
        command = var.cloud_run_config.command
        args    = var.cloud_run_config.args

        dynamic "env" {
          for_each = var.cloud_run_config.env_vars
          content {
            name  = env.key
            value = env.value
          }
        }

        ports {
          container_port = var.cloud_run_config.container_port
        }
      }

      service_account_name = var.cloud_run_config.service_account
    }

    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale" = "5"
        "run.googleapis.com/vpc-access-connector" = var.cloud_run_config.vpc_connector
        "run.googleapis.com/vpc-access-egress" = "all-traffic"
      }

    }
  }
  traffic {
    percent         = 100
    latest_revision = true
  }
}

# Pub/Sub topic (for triggering pipelines)
module "pubsub" {
  source     = "terraform-google-modules/pubsub/google"
  project_id = var.project_id
  topic      = var.pubsub_topic_name

  grant_token_creator = false
  create_subscriptions = true

  push_subscriptions = [
    {
      name                        = var.pubsub_topic_name
      ack_deadline_seconds        = var.pubsub_subscr_ack_deadline
      oidc_service_account_email  = var.pubsub_service_account
      push_endpoint               = google_cloud_run_service.run_service.status[0].url
    },
  ]

  depends_on = [
    google_cloud_run_service.run_service,
  ]
}
