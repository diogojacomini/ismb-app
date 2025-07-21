resource "aws_s3_bucket" "ismb_bucket" {
  bucket = var.bucket_name

  tags = {
    Name        = "ISMB Project Bucket"
    Environment = var.environment
    Project     = "ismb-app"
  }
}

resource "aws_s3_bucket_versioning" "ismb_bucket_versioning" {
  bucket = aws_s3_bucket.ismb_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "ismb_bucket_encryption" {
  bucket = aws_s3_bucket.ismb_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "ismb_bucket_pab" {
  bucket = aws_s3_bucket.ismb_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Estrutura dos arquivos
resource "aws_s3_object" "data_folders" {
  for_each = toset([
    "data/sandbox/",
    "data/bronze/",
    "data/silver/",
    "data/gold/"
  ])

  bucket       = aws_s3_bucket.ismb_bucket.id
  key          = each.value
  content_type = "application/x-directory"
}
