variable "aws_region" {
	description = "AWS region to deploy resources into"
	type        = string
	default     = "us-east-1"
}

variable "s3_bucket_name" {
	description = "Name of the S3 bucket to create"
	type        = string
	default     = "meu-bucket-ismb"
}

