variable "subscription_id" {
  description = "Azure subscription to deploy into."
  type        = string
  default     = null
}

variable "project" {
  description = "Short name used in resource names."
  type        = string
  default     = "retaillake"

  validation {
    condition     = can(regex("^[a-z0-9]{3,12}$", var.project))
    error_message = "3-12 lowercase letters or digits (it becomes part of a storage account name)."
  }
}

variable "environment" {
  description = "dev, test or prod."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "One of dev, test, prod."
  }
}

variable "location" {
  description = "Azure region."
  type        = string
  default     = "australiaeast"
}

variable "owner" {
  description = "Who to ask about these resources (a tag)."
  type        = string
  default     = "data-platform"
}

variable "databricks_sku" {
  description = "premium is required for Unity Catalog."
  type        = string
  default     = "premium"
}

variable "node_type" {
  description = "VM size of the job cluster's single node."
  type        = string
  default     = "Standard_D4ds_v6" # v5 had no quota and v4 no capacity on a new subscription (week-05.md)
}

variable "wheel_version" {
  description = "Version of the retail_lakehouse wheel uploaded to the artifacts volume by the deploy workflow."
  type        = string
  default     = "0.1.0"
}

variable "grant_deployer_data_access" {
  description = "Give the identity running tofu Storage Blob Data Contributor on the lake (to upload drops and the wheel)."
  type        = bool
  default     = true
}

variable "alert_email" {
  description = "Where job failures are emailed."
  type        = string
  default     = "data-oncall@example.com"
}
