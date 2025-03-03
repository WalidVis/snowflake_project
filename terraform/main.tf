terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = ">= 1.0.0"
    }
  }

  backend "remote" {
    organization = "orga"

    workspaces {
      prefix = "snowflake-project-"
    }
  }
}

provider "snowflake" {
    organization_name = "viseo.com"
    account_name      = "Viseo_admin"
    user              = "VISEO"
    password          = "TESTTESTtest123"
    role              = "ACCOUNTADMIN"

}
