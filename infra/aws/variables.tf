variable "region" {
  description = "AWS region."
  type        = string
  default     = "ap-southeast-2"
}

variable "project" {
  description = "Short name used in resource names."
  type        = string
  default     = "retail-lakehouse"
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

variable "owner" {
  description = "Owner tag."
  type        = string
  default     = "Luke458"
}

variable "wheel_version" {
  description = "Version of the retail_lakehouse wheel the Glue job installs from the artifacts prefix."
  type        = string
  default     = "0.1.0"
}

variable "glue_workers" {
  description = "G.1X workers per Glue job run (2 is Glue's minimum: one driver, one executor)."
  type        = number
  default     = 2

  validation {
    condition     = var.glue_workers >= 2 && var.glue_workers <= 4
    error_message = "2 to 4 workers: more needs the author's word first (cost)."
  }
}

variable "glue_timeout_minutes" {
  description = "A Glue run is stopped (and billed no further) after this many minutes."
  type        = number
  default     = 30
}

variable "athena_scan_limit_bytes" {
  description = "Athena cancels any query in the workgroup that would scan more than this."
  type        = number
  default     = 1073741824 # 1 GiB, about US$0.005 at US$5 per TB
}
