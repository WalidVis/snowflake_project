resource "snowflake_user" "user1" {
    provider          = snowflake.security_admin
    name              = "walid_cheriet"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_role.role.name
    default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "user2" {
    provider          = snowflake.security_admin
    name              = "saif_bouaziz"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_role.role.name
    default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "user3" {
    provider          = snowflake.security_admin
    name              = "zied_maalej"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_role.role.name
    default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "user4" {
    provider          = snowflake.security_admin
    name              = "issam_eddaou"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_role.role.name
    default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "user5" {
    provider          = snowflake.security_admin
    name              = "rihab_bahri"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_role.role.name
    default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "user6" {
    provider          = snowflake.security_admin
    name              = "matthieu_noiret"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_role.role.name
    default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}

resource "snowflake_user" "user7" {
    provider          = snowflake.security_admin
    name              = "amine_snoussi"
    default_warehouse = snowflake_warehouse.warehouse.name
    default_role      = snowflake_role.role.name
    default_namespace = "${snowflake_database.db.name}.${snowflake_schema.schema.name}"
   // rsa_public_key    = substr(tls_private_key.svc_key.public_key_pem, 27, 398)
}