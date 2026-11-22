variable "aws_region" {
  description = "AWS region for the deployment."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "dev"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name for the lakehouse."
  type        = string
}

variable "force_destroy" {
  description = "Allow destroying a non-empty bucket (dev only)."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Additional resource tags."
  type        = map(string)
  default     = {}
}
