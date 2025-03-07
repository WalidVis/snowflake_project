#To set lots of variables, more convenient to specify their values in a
#variable def. file (with filename ending in either .tfvars OR .tfvars.json)
#and then specify that file on  command line with -var-file
#Terraform automatically loads number of variable files if they are present in the root module :
# - Files named exactly terraform.tfvars or terraform.tfvars.json.
# - Any files with names ending in .auto.tfvars or .auto.tfvars.json.

environment    = "DEV"
warehouse_size = "x-small"
