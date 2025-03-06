resource "snowflake_account_role" "role" {
  provider = snowflake.security_admin
  name     = "${var.environment}_POC_VISEO_ROLE"
  comment  = "Main role for Snowflake POC account usage"
}

resource "snowflake_grant_account_role" "rolegrant" {
  provider         = snowflake.security_admin
  role_name        = snowflake_account_role.role.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "grants" {
  provider  = snowflake.security_admin
  role_name = snowflake_account_role.role.name
  user_name = "TERRAF_USER"
  //user_name = snowflake_user.user.name
}