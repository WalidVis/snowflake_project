terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = ">= 1.0.0"
    }
  }

  backend "remote" {
    organization = "orga_vis"

    workspaces {
      name = "snowflake_project_workspace"
    }
  }
}

provider "snowflake" {


}
