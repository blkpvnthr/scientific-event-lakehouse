variable "bucket_name" {
  description = "Globally unique S3 bucket name for the lakehouse."
  type        = string
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)."
  type        = string
}

variable "enable_versioning" {
  description = "Enable S3 object versioning."
  type        = bool
  default     = true
}

variable "noncurrent_version_expiration_days" {
  description = "Days after which noncurrent object versions expire."
  type        = number
  default     = 90
}

variable "force_destroy" {
  description = "Allow Terraform to destroy a non-empty bucket. Keep false in prod."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Resource tags."
  type        = map(string)
  default     = {}
}
