# Snowflake objects for a second gold target: the loader copies approved
# weeks of silver into SILVER, and dbt builds GOLD from them.

locals {
  comment = "retail-lakehouse (managed by OpenTofu, github.com/Luke458/retail-lakehouse)"
}

resource "snowflake_database" "lake" {
  name                        = var.database
  comment                     = local.comment
  data_retention_time_in_days = 1
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.lake.name
  name     = "SILVER"
  comment  = "Gate-approved weeks of silver, loaded from the local Delta lake."
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.lake.name
  name     = "GOLD"
  comment  = "dbt gold models (the same project that builds gold on DuckDB)."
}

# Spend cap. Notifications at 50% and 75%, suspend after running queries
# finish at 90%, suspend immediately at 100%.
resource "snowflake_resource_monitor" "cap" {
  name                      = "RETAIL_LAKEHOUSE_CAP"
  credit_quota              = var.credit_quota
  notify_triggers           = [50, 75]
  suspend_trigger           = 90
  suspend_immediate_trigger = 100
}

resource "snowflake_warehouse" "transform" {
  name                = "TRANSFORM_XS"
  warehouse_size      = var.warehouse_size
  auto_suspend        = var.auto_suspend_seconds
  auto_resume         = "true"
  initially_suspended = true
  resource_monitor    = snowflake_resource_monitor.cap.name
  comment             = local.comment
}

# Roles: LOADER writes SILVER, TRANSFORMER reads SILVER and writes GOLD.
# Both sit under SYSADMIN, so the admin roles can see what they own.
resource "snowflake_account_role" "loader" {
  name    = "LOADER"
  comment = "Loads gate-approved silver weeks into SILVER."
}

resource "snowflake_account_role" "transformer" {
  name    = "TRANSFORMER"
  comment = "Builds GOLD with dbt from SILVER."
}

resource "snowflake_grant_account_role" "to_sysadmin" {
  for_each         = { loader = snowflake_account_role.loader.name, transformer = snowflake_account_role.transformer.name }
  role_name        = each.value
  parent_role_name = "SYSADMIN"
}

# Service users: key-pair only, no password, no interactive login.
resource "snowflake_service_user" "loader" {
  name              = "LAKEHOUSE_LOADER"
  default_role      = snowflake_account_role.loader.name
  default_warehouse = snowflake_warehouse.transform.name
  default_namespace = "${snowflake_database.lake.name}.${snowflake_schema.silver.name}"
  rsa_public_key    = var.loader_rsa_public_key
  comment           = local.comment
}

resource "snowflake_service_user" "dbt" {
  name              = "LAKEHOUSE_DBT"
  default_role      = snowflake_account_role.transformer.name
  default_warehouse = snowflake_warehouse.transform.name
  default_namespace = "${snowflake_database.lake.name}.${snowflake_schema.gold.name}"
  rsa_public_key    = var.dbt_rsa_public_key
  comment           = local.comment
}

resource "snowflake_grant_account_role" "loader_user" {
  role_name = snowflake_account_role.loader.name
  user_name = snowflake_service_user.loader.name
}

resource "snowflake_grant_account_role" "dbt_user" {
  role_name = snowflake_account_role.transformer.name
  user_name = snowflake_service_user.dbt.name
}
