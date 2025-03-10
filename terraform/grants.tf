resource "snowflake_grant_account_role" "grant_poc_viseo_role_sbouaziz" {
  provider  = snowflake.security_admin
  role_name = snowflake_account_role.role.name
  user_name = snowflake_user.saif_bouaziz.name
}

resource "snowflake_grant_account_role" "grant_poc_viseo_role_wcheriet" {
  provider  = snowflake.security_admin
  role_name = snowflake_account_role.role.name
  user_name = snowflake_user.walid_cheriet.name
}

resource "snowflake_grant_account_role" "grant_poc_viseo_role_zmaalej" {
  provider  = snowflake.security_admin
  role_name = snowflake_account_role.role.name
  user_name = snowflake_user.zied_maalej.name
}

resource "snowflake_grant_account_role" "grant_poc_viseo_role_ieddaou" {
  provider  = snowflake.security_admin
  role_name = snowflake_account_role.role.name
  user_name = snowflake_user.issam_eddaou.name
}

resource "snowflake_grant_account_role" "grant_poc_viseo_role_rbahri" {
  provider  = snowflake.security_admin
  role_name = snowflake_account_role.role.name
  user_name = snowflake_user.rihab_bahri.name
}

resource "snowflake_grant_account_role" "grant_poc_viseo_role_mnoiret" {
  provider  = snowflake.security_admin
  role_name = snowflake_account_role.role.name
  user_name = snowflake_user.matthieu_noiret.name
}

resource "snowflake_grant_account_role" "grant_poc_viseo_role_asnoussi" {
  provider  = snowflake.security_admin
  role_name = snowflake_account_role.role.name
  user_name = snowflake_user.amine_snoussi.name
}