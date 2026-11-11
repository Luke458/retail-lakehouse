terraform {
  required_version = ">= 1.8"

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.23"
    }
  }

  # Local state: one trial account, one person. State holds only public keys.
  backend "local" {
    path = "terraform.tfstate"
  }
}

# Connection comes from the environment (scripts/snowflake.sh sets it from
# ~/.config/retail-lakehouse/snowflake.env): SNOWFLAKE_ORGANIZATION_NAME,
# SNOWFLAKE_ACCOUNT_NAME, SNOWFLAKE_USER, SNOWFLAKE_ROLE,
# SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT and SNOWFLAKE_PRIVATE_KEY.
# Nothing account-specific is in the repo.
provider "snowflake" {}
