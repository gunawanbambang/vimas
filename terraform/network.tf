# VPC Network for Cloud Composer & GKE Autopilot
resource "google_compute_network" "composer_network" {
  name                    = "composer-vpc"
  auto_create_subnetworks = false
  project                 = var.project_id
}

# Subnetwork in asia-southeast2 with secondary IP ranges for Pods and Services
resource "google_compute_subnetwork" "composer_subnet" {
  name                     = "composer-subnet-jkt"
  ip_cidr_range            = "10.10.0.0/20"
  region                   = var.region
  network                  = google_compute_network.composer_network.id
  private_ip_google_access = true
  project                  = var.project_id

  secondary_ip_range {
    range_name    = "composer-pods"
    ip_cidr_range = "10.20.0.0/16"
  }

  secondary_ip_range {
    range_name    = "composer-services"
    ip_cidr_range = "10.30.0.0/20"
  }
}

# Firewall rule allowing internal cluster communication
resource "google_compute_firewall" "internal_ingress" {
  name        = "allow-internal-composer"
  network     = google_compute_network.composer_network.name
  project     = var.project_id
  description = "Allow internal traffic within Composer VPC"

  allow {
    protocol = "icmp"
  }
  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  source_ranges = ["10.0.0.0/8"]
}
