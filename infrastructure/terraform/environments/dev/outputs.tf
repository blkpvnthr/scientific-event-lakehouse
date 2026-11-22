output "lakehouse_bucket" {
  description = "Name of the lakehouse S3 bucket."
  value       = module.lakehouse_bucket.bucket_id
}

output "lakehouse_bucket_arn" {
  description = "ARN of the lakehouse S3 bucket."
  value       = module.lakehouse_bucket.bucket_arn
}

output "pipeline_role_arn" {
  description = "ARN of the least-privilege pipeline IAM role."
  value       = module.pipeline_iam.role_arn
}
