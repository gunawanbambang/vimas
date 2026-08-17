terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Ensure all required Google Cloud APIs are enabled
locals {
  required_services = [
    "composer.googleapis.com",
    "container.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "secretmanager.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com"
  ]
}

resource "google_project_service" "apis" {
  for_each           = toset(local.required_services)
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}
