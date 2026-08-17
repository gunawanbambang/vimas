variable "project_id" {
  description = "Google Cloud Project ID"
  type        = string
  default     = "elevate-505410"
}

variable "region" {
  description = "Google Cloud Region"
  type        = string
  default     = "asia-southeast2"
}

variable "gsa_name" {
  description = "Name of the Google Service Account for Pod processing"
  type        = string
  default     = "flatfile-pod-processor-sa"
}

variable "ksa_name" {
  description = "Name of the Kubernetes Service Account in Composer"
  type        = string
  default     = "flatfile-processor-ksa"
}

variable "k8s_namespace" {
  description = "Target Kubernetes namespace for Composer user workloads"
  type        = string
  default     = "composer-user-workloads"
}
