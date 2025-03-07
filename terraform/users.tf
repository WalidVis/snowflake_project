/*resource "snowflake_user" "walid_cheriet" {
    provider          = snowflake.security_admin
    name              = "walid_cheriet"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_account_role.role.name
    //  default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "saif_bouaziz" {
    provider          = snowflake.security_admin
    name              = "saif_bouaziz"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_account_role.role.name
   //   default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "zied_maalej" {
    provider          = snowflake.security_admin
    name              = "zied_maalej"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_account_role.role.name
    //  default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "issam_eddaou" {
    provider          = snowflake.security_admin
    name              = "issam_eddaou"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_account_role.role.name
    //  default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "rihab_bahri" {
    provider          = snowflake.security_admin
    name              = "rihab_bahri"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_account_role.role.name
    //  default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "matthieu_noiret" {
    provider          = snowflake.security_admin
    name              = "matthieu_noiret"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_account_role.role.name
    //  default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "amine_snoussi" {
    provider          = snowflake.security_admin
    name              = "amine_snoussi"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_account_role.role.name
   // default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}
*/

resource "snowflake_user" "test_user" {
    provider = snowflake.security_admin
    name = "test_user"
    login_name = "test_user"
    comment = "User account for : test_user (DEV_POC_VISEO_ROLE)"
    password = "Temporarypassword123!"
}