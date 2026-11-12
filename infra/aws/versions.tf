terraform {
  required_version = ">= 1.8"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.73"
    }
  }

  # Local state: one account, one person, applied and destroyed in a session.
  backend "local" {
    path = "terraform.tfstate"
  }
}

# Credentials come from the environment (AWS_PROFILE, an `aws login` or SSO
# session). No keys in files, nothing account-specific in the repo.
provider "aws" {
  region = var.region

  default_tags {
    tags = local.tags
  }
}
