resource "snowflake_grant_account_role" "grant_poc_viseo_role" {
  provider  = snowflake.security_admin
  role_name = snowflake_account_role.role.name
  user_name = snowflake_user.saif_bouaziz.name
}