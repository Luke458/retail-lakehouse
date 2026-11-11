output "database" {
  value = snowflake_database.lake.name
}

output "warehouse" {
  value = snowflake_warehouse.transform.name
}

output "loader_user" {
  value = snowflake_service_user.loader.name
}

output "dbt_user" {
  value = snowflake_service_user.dbt.name
}

output "resource_monitor" {
  value = snowflake_resource_monitor.cap.name
}
