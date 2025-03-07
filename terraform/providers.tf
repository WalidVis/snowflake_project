

# Provider required schema parameters:
#-------------------------------------
# organization_name = "..." # Not using profile -> Defined via Terraform cloud environment var SNOWFLAKE_ORGANIZATION_NAME 
# account_name     = "..." # Not using profile -> Defined on Terraform cloud environment var SNOWFLAKE_ACCOUNT_NAME
# user              = "..." # Not using profile -> Defined on Terraform cloud environment var SNOWFLAKE_USER 

provider "snowflake" {
  alias = "security_admin"
  role  = "SECURITYADMIN"
  //   profile  = "securityadmin"
}

provider "snowflake" {
  alias = "sys_admin"
  role  = "SYSADMIN"
}

provider "snowflake" {
  alias = "account_admin"
  role  = "ACCOUNTADMIN"
}