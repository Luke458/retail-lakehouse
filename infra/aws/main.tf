# The Week 5 pattern on AWS: a lake bucket (Delta tables under tables/, the
# vendor drops, artifacts and reports under files/), a Glue Spark job that
# runs the same wheel, a Glue Data Catalog database and an Athena workgroup
# to query silver.

data "aws_caller_identity" "current" {}

locals {
  tags = {
    project     = var.project
    environment = var.environment
    owner       = var.owner
    managed_by  = "opentofu"
    repo        = "github.com/Luke458/retail-lakehouse"
  }
  # Bucket names are global: the account id keeps them unique without a random suffix.
  name           = "${var.project}-${var.environment}"
  lake_bucket    = "${local.name}-lake-${data.aws_caller_identity.current.account_id}"
  results_bucket = "${local.name}-athena-${data.aws_caller_identity.current.account_id}"
}

# --- Lake bucket -------------------------------------------------------------

resource "aws_s3_bucket" "lake" {
  bucket = local.lake_bucket
  # Applied, run once, destroyed the same day: destroy empties it.
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Delta rewrites files on every overwrite and VACUUM; old versions go after a day.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket     = aws_s3_bucket.lake.id
  depends_on = [aws_s3_bucket_versioning.lake]

  rule {
    id     = "expire-old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 1
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# --- Athena -------------------------------------------------------------------

resource "aws_s3_bucket" "athena_results" {
  bucket        = local.results_bucket
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "expire-results"
    status = "Enabled"
    filter {}
    expiration {
      days = 7
    }
  }
}

resource "aws_athena_workgroup" "lake" {
  name          = local.name
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query     = var.athena_scan_limit_bytes

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# --- Glue Data Catalog -----------------------------------------------------------

resource "aws_glue_catalog_database" "lake" {
  name        = replace(local.name, "-", "_")
  description = "Silver Delta tables of retail-lakehouse, for Athena."
}
