# snowflake_project


To use multiple remote workspaces, set workspaces.prefix to a prefix used in all of the desired remote workspace names. For example, set prefix = "networking-" to use HCP Terraform workspaces with names like networking-dev and networking-prod. This is helpful when mapping multiple Terraform CLI workspaces used in a single Terraform configuration to multiple HCP Terraform workspaces.

The backend configuration requires either name or prefix. Omitting both or setting both results in a configuration error.

Workspace Names
Terraform uses shortened names without the common prefix to interact with workspaces on the command line. For example, if prefix = "networking-", use terraform workspace select prod to switch to the Terraform CLI workspace prod within the current configuration. However, remote Terraform operations such as plan and apply for that Terraform CLI workspace will take place in the HCP Terraform workspace networking-prod.