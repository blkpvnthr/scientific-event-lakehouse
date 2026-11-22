# Least-privilege IAM role + policy for the lakehouse pipeline.

data "aws_iam_policy_document" "assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = var.assume_role_services
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "${var.name_prefix}-pipeline-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
  tags               = merge(var.tags, { Environment = var.environment })
}

# Scoped to exactly one bucket: list the bucket, read/write/delete its objects.
data "aws_iam_policy_document" "access" {
  statement {
    sid       = "ListLakehouseBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.bucket_arn]
  }

  statement {
    sid    = "ReadWriteLakehouseObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = ["${var.bucket_arn}/*"]
  }
}

resource "aws_iam_policy" "access" {
  name   = "${var.name_prefix}-pipeline-access"
  policy = data.aws_iam_policy_document.access.json
  tags   = merge(var.tags, { Environment = var.environment })
}

resource "aws_iam_role_policy_attachment" "access" {
  role       = aws_iam_role.pipeline.name
  policy_arn = aws_iam_policy.access.arn
}
