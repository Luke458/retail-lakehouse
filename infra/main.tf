# Azure: a resource group, the lake (ADLS Gen2), a Databricks workspace, and
# the identity Databricks uses to reach the lake.

locals {
  name = "${var.project}-${var.environment}"
  tags = {
    project     = var.project
    environment = var.environment
    owner       = var.owner
    managed_by  = "opentofu"
    repo        = "retail-lakehouse"
  }
}

resource "azurerm_resource_group" "this" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = local.tags
}

# The lake: hierarchical namespace on (ADLS Gen2), so directories are real and
# renames are atomic. Locally-redundant: the lake can be rebuilt from the
# vendor drops, and the drops are kept in their own layer.
resource "azurerm_storage_account" "lake" {
  name                            = "st${var.project}${var.environment}"
  resource_group_name             = azurerm_resource_group.this.name
  location                        = azurerm_resource_group.this.location
  account_kind                    = "StorageV2"
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  shared_access_key_enabled       = false # identities only, no account keys
  allow_nested_items_to_be_public = false
  tags                            = local.tags
}

# One container per role. "files" holds the vendor drops, raw files, reports
# and control records (a Unity Catalog volume); "tables" holds the Delta
# tables, bronze to gold.
resource "azurerm_storage_container" "lake" {
  for_each              = toset(["files", "tables"])
  name                  = each.key
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# Lifecycle: vendor drops and raw files are kept, but move to cool storage
# after 30 days (read again only to replay a week).
resource "azurerm_storage_management_policy" "lake" {
  storage_account_id = azurerm_storage_account.lake.id

  rule {
    name    = "cool-old-drops"
    enabled = true
    filters {
      blob_types   = ["blockBlob"]
      prefix_match = ["files/inbox/", "files/raw/"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 30
      }
    }
  }
}

resource "azurerm_databricks_workspace" "this" {
  name                        = "dbw-${local.name}"
  resource_group_name         = azurerm_resource_group.this.name
  location                    = azurerm_resource_group.this.location
  sku                         = var.databricks_sku
  managed_resource_group_name = "rg-${local.name}-databricks-managed"
  tags                        = local.tags
}

# Databricks reaches the lake as this managed identity, through Unity Catalog:
# no secrets in cluster configs.
resource "azurerm_databricks_access_connector" "lake" {
  name                = "dbac-${local.name}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "connector_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.lake.identity[0].principal_id
}
