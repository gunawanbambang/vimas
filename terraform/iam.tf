# Dedicated Google Service Account for Pod processing
resource "google_service_account" "pod_sa" {
  account_id   = var.gsa_name
  display_name = "Service Account for GKE Pod Flat File Decompression"
  description  = "Least-privilege service account used by flatfile-processor Kubernetes pods"
  project      = var.project_id
}

# Storage Object Admin on Landing Bucket
resource "google_storage_bucket_iam_member" "landing_access" {
  bucket = google_storage_bucket.landing_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pod_sa.email}"
}

# Storage Object Admin on Staging Bucket
resource "google_storage_bucket_iam_member" "staging_access" {
  bucket = google_storage_bucket.staging_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pod_sa.email}"
}

# Secret Manager Secret for SFTP GPG Private Key
resource "google_secret_manager_secret" "gpg_key" {
  secret_id = "sftp-gpg-private-key"
  project   = var.project_id

  replication {
    auto {}
  }
}

# Secret Accessor permission for Pod Service Account
resource "google_secret_manager_secret_iam_member" "secret_access" {
  secret_id = google_secret_manager_secret.gpg_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pod_sa.email}"
  project   = var.project_id
}

# Workload Identity binding with dedicated Kubernetes Service Account
resource "google_service_account_iam_member" "workload_identity_binding" {
  service_account_id = google_service_account.pod_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${var.k8s_namespace}/${var.ksa_name}]"
}
