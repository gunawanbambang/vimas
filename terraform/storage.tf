# Cloud Storage Landing Bucket (Inbound archives from SFTP)
resource "google_storage_bucket" "landing_bucket" {
  name                        = "${var.project_id}-flatfile-landing"
  location                    = var.region
  project                     = var.project_id
  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30 # Retain landing archives for 30 days
    }
    action {
      type = "Delete"
    }
  }
}

# Cloud Storage Staging Bucket (Uncompressed flat files for BQ direct load)
resource "google_storage_bucket" "staging_bucket" {
  name                        = "${var.project_id}-flatfile-staging"
  location                    = var.region
  project                     = var.project_id
  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    condition {
      age = 14 # Retain staging flat files for 14 days post-load
    }
    action {
      type = "Delete"
    }
  }
}

# Artifact Registry Repository for Data Pipeline container images
resource "google_artifact_registry_repository" "data_pipelines" {
  provider      = google
  location      = var.region
  repository_id = "data-pipelines"
  description   = "Docker container images for data ingestion and pod-offloaded processing"
  format        = "DOCKER"
  project       = var.project_id
}
