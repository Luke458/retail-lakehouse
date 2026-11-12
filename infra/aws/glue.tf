# One Glue Spark job runs every step (bronze, silver, reconcile, reference),
# chosen per run with --step, as the Databricks job ran one task per step.
# Airflow starts it per week and owns retries, so the job itself never retries.

resource "aws_s3_object" "glue_entry" {
  bucket = aws_s3_bucket.lake.id
  key    = "files/artifacts/glue_entry.py"
  source = "${path.module}/glue_entry.py"
  etag   = filemd5("${path.module}/glue_entry.py")
}

resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws-glue/jobs/${local.name}"
  retention_in_days = 7
}

resource "aws_glue_job" "spark" {
  name              = "${local.name}-spark"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "6.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  timeout           = var.glue_timeout_minutes
  max_retries       = 0
  execution_class   = "STANDARD"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.glue_entry.key}"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 2 # sales and transactions side by side
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "delta"
    "--conf"                             = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.sql.session.timeZone=UTC"
    "--additional-python-modules"        = "s3://${aws_s3_bucket.lake.bucket}/files/artifacts/retail_lakehouse-${var.wheel_version}-py3-none-any.whl,duckdb==2.0.0"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--TABLE_ROOT"                       = "s3://${aws_s3_bucket.lake.bucket}/tables"
    "--LAKE_URI"                         = "s3://${aws_s3_bucket.lake.bucket}/files"
  }
}
