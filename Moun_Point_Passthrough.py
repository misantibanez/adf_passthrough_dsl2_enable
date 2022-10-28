# Databricks notebook source
# MAGIC %md
# MAGIC #Definición de variables

# COMMAND ----------

# MAGIC %md
# MAGIC Se sugiere definir las variables asociadas al storage, container y archivos que se van a consumir.

# COMMAND ----------

storage_account_name = 'sademoaml'
container_name = 'fsdata'
filename = 'UsedCars.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC Se sugiere establer el nombre del punto de montura que se va a utilizar.

# COMMAND ----------

mount_name = 'sademoamlmp'

# COMMAND ----------

# MAGIC %md
# MAGIC #Consumo directo desde el Storage Account

# COMMAND ----------

# MAGIC %md
# MAGIC El usuario que ejecuta la celda (cmd) debe tener acceso al menos de Storage Blob Reader en el Storage Account.

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("abfss://"+container_name+"@"+storage_account_name+".dfs.core.windows.net/"+filename)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Punto de Montura usando Passthrough

# COMMAND ----------

# MAGIC %md
# MAGIC Si bien el punto de montura se va a crear, el usuario que ejecuta una consulta sobre los datos dentro del punto de montura deberán tener permisos desde el Storage Account, al menos Storage Blob Data Contributor.

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://"+container_name+"@"+storage_account_name+".dfs.core.windows.net/",
  mount_point = "/mnt/"+mount_name,
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC Validación del punto de montura

# COMMAND ----------

dbutils.fs.ls("/mnt/"+mount_name) 

# COMMAND ----------

# MAGIC %md
# MAGIC Lectura del archivo desde el punto de montura

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/sademoamlmp/UsedCars.csv")
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Eliminación de punto de montura

# COMMAND ----------

dbutils.fs.unmount("/mnt/"+mount_name) 
