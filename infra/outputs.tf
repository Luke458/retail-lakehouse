output "resource_group" {
  value = azurerm_resource_group.this.name
}

output "storage_account" {
  value = azurerm_storage_account.lake.name
}

output "tables_root" {
  description = "TABLE_ROOT for the Spark jobs: where the Delta tables live."
  value       = local.tables_root
}

output "files_root" {
  description = "LAKE_ROOT for the Spark jobs: the Unity Catalog volume over the files container."
  value       = local.volume_root
}

output "databricks_workspace_url" {
  value = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "transform_job_id" {
  description = "For Airflow's DatabricksRunNowOperator."
  value       = databricks_job.transform_week.id
}
