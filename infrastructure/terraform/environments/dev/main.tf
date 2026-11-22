terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "scientific-event-lakehouse"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

module "lakehouse_bucket" {
  source = "../../modules/s3_bucket"

  bucket_name                        = var.bucket_name
  environment                        = var.environment
  enable_versioning                  = true
  noncurrent_version_expiration_days = 90
  # Dev convenience only; must be false in staging/prod.
  force_destroy = var.force_destroy
  tags          = var.tags
}

module "pipeline_iam" {
  source = "../../modules/iam"

  name_prefix = "selh-${var.environment}"
  environment = var.environment
  bucket_arn  = module.lakehouse_bucket.bucket_arn
  tags        = var.tags
}
