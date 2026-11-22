output "role_arn" {
  description = "ARN of the pipeline IAM role."
  value       = aws_iam_role.pipeline.arn
}

output "role_name" {
  description = "Name of the pipeline IAM role."
  value       = aws_iam_role.pipeline.name
}

output "policy_arn" {
  description = "ARN of the least-privilege access policy."
  value       = aws_iam_policy.access.arn
}
