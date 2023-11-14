# Databricks notebook source
# MAGIC %md
# MAGIC # Mounting Azure ADLS Gen2 Storage
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC * Utilize external cloud storage as staging area for FHIR data drops to enable "push into cloud" scenario so that Autoloader can be used
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC * This project and documentation is based on using Microsoft Azure Cloud Storage
# MAGIC
# MAGIC - This script only needs to **run once per storage account** you need to connect to
# MAGIC
# MAGIC **References**
# MAGIC
# MAGIC * [Mounting cloud object storage on Azure Databricks - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts)

# COMMAND ----------

# Setup variables - Replace with relevant values for your environment
storage_account_name="Azure storage account name"
storage_container_name="Azure storage container name"
application_id="Application ID for the Azure Active Directory app"
directory_id="Azure directory ID where storage account resides"
mount_name="name of subdirectory under /mnt/"
secret_scope_name="Databricks secert scope name"
service_credential_key_name="name of the secret key containing the client secret"

# Nothing below should need to be changed if all parameters are setup correctly
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope=secret_scope_name,key=service_credential_key_name),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

dbutils.fs.mount(
  source = f"abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{mount_name}}",
  extra_configs = configs)

dbutils.fs.ls(mount_name)
