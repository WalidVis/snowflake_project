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