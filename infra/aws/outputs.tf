output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "table_root" {
  description = "TABLE_ROOT for the Spark jobs."
  value       = "s3://${aws_s3_bucket.lake.bucket}/tables"
}

output "lake_uri" {
  description = "Where the files side (drops, artifacts, reports) lives."
  value       = "s3://${aws_s3_bucket.lake.bucket}/files"
}

output "glue_job" {
  value = aws_glue_job.spark.name
}

output "glue_database" {
  value = aws_glue_catalog_database.lake.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lake.name
}
