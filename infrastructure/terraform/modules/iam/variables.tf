variable "name_prefix" {
  description = "Prefix for IAM resource names."
  type        = string
}

variable "environment" {
  description = "Deployment environment."
  type        = string
}

variable "bucket_arn" {
  description = "ARN of the lakehouse S3 bucket the pipeline may access."
  type        = string
}

variable "assume_role_services" {
  description = "AWS service principals allowed to assume the pipeline role."
  type        = list(string)
  default     = ["ec2.amazonaws.com"]
}

variable "tags" {
  description = "Resource tags."
  type        = map(string)
  default     = {}
}
