# https://developer.hashicorp.com/terraform/language/values/variables

variable "environment" {
  type        = string
  description = "Deployment env ex: DEV, TEST, QUAL, PROD"
}

variable "warehouse_size" {
  type        = string
  description = "warehouse size"
}

variable "SNOWFLAKE_ACCOUNT_NAME" {
  type        = string
  description = "warehouse size"
  default = ""
}

variable "SNOWFLAKE_ORGANIZATION_NAME" {
  type        = string
  description = "warehouse size"
  default = ""
}