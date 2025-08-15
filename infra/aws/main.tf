locals {
	bucket_name = var.s3_bucket_name
}

resource "aws_s3_bucket" "this" {
	bucket        = local.bucket_name
	force_destroy = false
}

resource "aws_s3_bucket_ownership_controls" "this" {
	bucket = aws_s3_bucket.this.id

	rule {
		object_ownership = "BucketOwnerPreferred"
	}
}

resource "aws_s3_bucket_acl" "this" {
	depends_on = [aws_s3_bucket_ownership_controls.this]
	bucket     = aws_s3_bucket.this.id
	acl        = "private"
}

resource "aws_s3_bucket_versioning" "this" {
	bucket = aws_s3_bucket.this.id

	versioning_configuration {
		status = "Enabled"
	}
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
	bucket = aws_s3_bucket.this.id

	rule {
		apply_server_side_encryption_by_default {
			sse_algorithm = "AES256"
		}
	}
}

resource "aws_s3_bucket_public_access_block" "this" {
	bucket = aws_s3_bucket.this.id

	block_public_acls       = true
	block_public_policy     = true
	ignore_public_acls      = true
	restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
	bucket = aws_s3_bucket.this.id

	rule {
		id     = "abort-incomplete-mpu"
		status = "Enabled"

		abort_incomplete_multipart_upload {
			days_after_initiation = 7
		}
	}
}

output "bucket_name" {
	value       = aws_s3_bucket.this.bucket
	description = "Created S3 bucket name"
}

output "bucket_arn" {
	value       = aws_s3_bucket.this.arn
	description = "Created S3 bucket ARN"
}

