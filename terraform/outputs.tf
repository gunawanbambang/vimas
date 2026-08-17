output "landing_bucket_url" {
  description = "GCS URL of the landing bucket"
  value       = google_storage_bucket.landing_bucket.url
}

output "staging_bucket_url" {
  description = "GCS URL of the staging bucket"
  value       = google_storage_bucket.staging_bucket.url
}

output "pod_service_account_email" {
  description = "Email of the dedicated pod service account"
  value       = google_service_account.pod_sa.email
}

output "artifact_registry_repo" {
  description = "Artifact Registry Docker repository path"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.data_pipelines.repository_id}"
}

output "secret_manager_secret_id" {
  description = "Secret Manager secret ID for GPG private key"
  value       = google_secret_manager_secret.gpg_key.secret_id
}
