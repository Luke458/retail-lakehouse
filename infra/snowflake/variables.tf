variable "database" {
  description = "Database holding the SILVER and GOLD schemas."
  type        = string
  default     = "RETAIL_LAKEHOUSE"
}

variable "warehouse_size" {
  description = "Size of the transform warehouse."
  type        = string
  default     = "XSMALL"
}

variable "auto_suspend_seconds" {
  description = "Idle seconds before the warehouse suspends (billing is per second, 60 s minimum per resume)."
  type        = number
  default     = 60
}

variable "credit_quota" {
  description = "Credits per month on the transform warehouse before the resource monitor suspends it."
  type        = number
  default     = 10

  validation {
    condition     = var.credit_quota > 0 && var.credit_quota <= 20
    error_message = "Between 1 and 20 credits: a bigger cap needs the author's word first."
  }
}

variable "loader_rsa_public_key" {
  description = "Public key (PEM body, no header lines) for the loader service user."
  type        = string
}

variable "dbt_rsa_public_key" {
  description = "Public key (PEM body, no header lines) for the dbt service user."
  type        = string
}
