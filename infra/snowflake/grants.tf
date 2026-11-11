# Least privilege: each role gets only what its job needs.

locals {
  roles = {
    loader      = snowflake_account_role.loader.name
    transformer = snowflake_account_role.transformer.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  for_each          = local.roles
  account_role_name = each.value
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  for_each          = local.roles
  account_role_name = each.value
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.lake.name
  }
}

# LOADER owns the SILVER tables it creates (and its stage and file format).
resource "snowflake_grant_privileges_to_account_role" "loader_silver" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE STAGE", "CREATE FILE FORMAT"]
  on_schema {
    schema_name = snowflake_schema.silver.fully_qualified_name
  }
}

# TRANSFORMER reads SILVER: schema usage, plus SELECT on every table the loader creates.
resource "snowflake_grant_privileges_to_account_role" "transformer_silver" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = snowflake_schema.silver.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_silver_tables" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.silver.fully_qualified_name
    }
  }
}

# TRANSFORMER owns GOLD's tables and views (dbt creates them).
resource "snowflake_grant_privileges_to_account_role" "transformer_gold" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = snowflake_schema.gold.fully_qualified_name
  }
}
