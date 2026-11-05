terraform {
  required_version = ">= 1.8"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.12"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.141"
    }
  }

  # Local state until there is a subscription. For a shared deployment, state
  # lives in a storage account of its own (not the lake's), with blob
  # versioning and a lease lock:
  #
  #   tofu init -backend-config=backend.azurerm.hcl
  #
  # after replacing this block with `backend "azurerm" {}`
  # (backend.azurerm.hcl.example shows the settings).
  backend "local" {
    path = "terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

# The workspace is created in the same apply, so the provider is configured
# from its attributes. On a first apply from scratch, run
# `tofu apply -target=azurerm_databricks_workspace.this` first.
provider "databricks" {
  host                        = azurerm_databricks_workspace.this.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.this.id
}
