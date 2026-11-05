# Unity Catalog over the lake, and the weekly transform as a Databricks job.
#
# Assumes the workspace has a Unity Catalog metastore assigned (new Azure
# workspaces get one automatically in most regions).

resource "databricks_storage_credential" "lake" {
  name    = "${local.name}-lake"
  comment = "Managed identity of ${azurerm_databricks_access_connector.lake.name}"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.lake.id
  }

  depends_on = [azurerm_role_assignment.connector_lake]
}

resource "databricks_external_location" "lake" {
  for_each        = azurerm_storage_container.lake
  name            = "${local.name}-${each.key}"
  url             = "abfss://${each.key}@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.lake.name
}

locals {
  files_root  = databricks_external_location.lake["files"].url
  tables_root = databricks_external_location.lake["tables"].url
}

resource "databricks_catalog" "retail" {
  name         = "retail_${var.environment}"
  storage_root = "${local.tables_root}managed"
  comment      = "Favorita weekly drops, bronze to gold"
}

resource "databricks_schema" "favorita" {
  catalog_name = databricks_catalog.retail.name
  name         = "favorita"
}

# Files the Python code reads and writes with ordinary paths (vendor drops,
# raw files, manifests, reports, control records, the deployed wheel):
# /Volumes/<catalog>/favorita/files/...
resource "databricks_volume" "files" {
  catalog_name     = databricks_catalog.retail.name
  schema_name      = databricks_schema.favorita.name
  name             = "files"
  volume_type      = "EXTERNAL"
  storage_location = local.files_root
}

locals {
  volume_root = "/Volumes/${databricks_catalog.retail.name}/${databricks_schema.favorita.name}/${databricks_volume.files.name}"
  wheel       = "${local.volume_root}/artifacts/retail_lakehouse-${var.wheel_version}-py3-none-any.whl"
  feeds       = ["sales", "transactions"]
  steps       = ["bronze", "silver", "reconcile"]
}

# One run per week: bronze -> silver -> reconcile for each feed, the same
# commands Airflow runs locally (`python -m retail_lakehouse.spark ...`).
# Airflow triggers it in the cloud (DatabricksRunNowOperator, job parameter
# `week`); the schedule stays in Airflow.
resource "databricks_job" "transform_week" {
  name                = "${local.name}-transform-favorita"
  max_concurrent_runs = 1 # the Delta writers run one at a time, as locally

  parameter {
    name    = "week"
    default = ""
  }

  job_cluster {
    job_cluster_key = "single_node"
    new_cluster {
      spark_version      = data.databricks_spark_version.lts.id
      node_type_id       = var.node_type
      num_workers        = 0
      data_security_mode = "SINGLE_USER"
      spark_conf = {
        "spark.master"                     = "local[*, 4]"
        "spark.databricks.cluster.profile" = "singleNode"
        "spark.sql.session.timeZone"       = "UTC"
      }
      custom_tags = merge(local.tags, { ResourceClass = "SingleNode" })
      spark_env_vars = {
        LAKE_ROOT  = local.volume_root
        TABLE_ROOT = local.tables_root
      }
    }
  }

  dynamic "task" {
    for_each = { for pair in setproduct(local.feeds, local.steps) : "${pair[1]}_${pair[0]}" => { feed = pair[0], step = pair[1] } }
    content {
      task_key        = task.key
      job_cluster_key = "single_node"

      dynamic "depends_on" {
        for_each = task.value.step == "bronze" ? [] : [task.value.step == "silver" ? "bronze" : "silver"]
        content {
          task_key = "${depends_on.value}_${task.value.feed}"
        }
      }

      python_wheel_task {
        package_name = "retail_lakehouse"
        entry_point  = "retail-lakehouse-spark"
        parameters = [
          task.value.step,
          "--feed", task.value.feed,
          "--week", "{{job.parameters.week}}",
          "--run-id", "databricks-{{job.run_id}}",
        ]
      }

      library {
        whl = local.wheel
      }
      library {
        pypi {
          package = "duckdb==2.0.0" # as in .venv-spark: reconcile's SQL side (the runtime ships pyarrow and pandas)
        }
      }

      max_retries               = task.value.step == "reconcile" ? 0 : 1
      min_retry_interval_millis = 60000
    }
  }

  email_notifications {
    on_failure = [var.alert_email]
  }

  tags = local.tags
}

data "databricks_spark_version" "lts" {
  long_term_support = true
}
