output "bucket_name" {
  description = "Name of the created S3 bucket"
  value       = aws_s3_bucket.ismb_bucket.bucket
}

output "bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.ismb_bucket.arn
}

output "bucket_domain_name" {
  description = "Domain name of the bucket"
  value       = aws_s3_bucket.ismb_bucket.bucket_domain_name
}
