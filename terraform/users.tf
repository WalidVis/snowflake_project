resource "snowflake_user" "walid_cheriet" {
    provider          = snowflake.security_admin
    name              = "WALID_CHERIET"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_account_role.role.name
    display_name = "walid_cheriet"
    password = "Temporarypassword123!"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "saif_bouaziz" {
    provider          = snowflake.security_admin
    name              = "SAIF_BOUAZIZ"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_account_role.role.name
    display_name = "saif_bouaziz"
    password = "Temporarypassword123!"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "zied_maalej" {
    provider          = snowflake.security_admin
    name              = "ZIED_MAALEJ"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_account_role.role.name
    display_name = "zied_maalej"
    password = "Temporarypassword123!"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "issam_eddaou" {
    provider          = snowflake.security_admin
    name              = "ISSAM_EDDAOU"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_account_role.role.name
    display_name = "issam_eddaou"
    password = "Temporarypassword123!"
}

resource "snowflake_user" "rihab_bahri" {
    provider          = snowflake.security_admin
    name              = "RIHAB_BAHRI"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_account_role.role.name
    display_name = "rihab_bahri"
    password = "Temporarypassword123!"
}

resource "snowflake_user" "matthieu_noiret" {
    provider          = snowflake.security_admin
    name              = "MATTHIEU_NOIRET"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_account_role.role.name
    display_name = "matthieu_noiret"
    password = "Temporarypassword123!"
}

resource "snowflake_user" "amine_snoussi" {
    provider          = snowflake.security_admin
    name              = "AMINE_SNOUSSI"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_account_role.role.name
    display_name = "amine_snoussi"
    password = "Temporarypassword123!"
   // default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}


resource "snowflake_user" "test_user" {
    provider = snowflake.security_admin
    name = "test_user"
    login_name = "test_user"
    comment = "User account for : test_user (DEV_POC_VISEO_ROLE)"
    password = "Temporarypassword123!"
}