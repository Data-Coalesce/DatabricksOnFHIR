# Databricks notebook source
#variable declarations
source_path="/mnt/fhir/"
bronze_table="fhir.bronze_all_resource_types"
checkpoint_path="/tmp/fhir/_checkpoint/bronze_all_resource_types"
#dbutils.fs.rm(checkpoint_path,True) #here only for resetting during dev/test

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ${bronze_table};

# COMMAND ----------

from pyspark.sql.functions import col,sha2,schema_of_json,concat,replace,lit,regexp_replace,concat,explode,from_json,array,current_timestamp
from pyspark.sql.types import StringType,StructType,_parse_datatype_string,StructField
df=(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("recursiveFileLookup","true")
  .option("cloudFiles.schemaLocation", checkpoint_path) 
 # .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
  .option("inferSchema","false") 
  #.option("mergeSchema","true")
  .option("multiLine", "false")
  .load(source_path)    
  .select("*"
          ,regexp_replace(regexp_replace("_metadata.file_path",source_path,""),concat(lit("/"),col("_metadata.file_name")),"").alias("client")
         # ,col("_metadata"),sha2(concat(col("client"),lit("|"),col("resourceType"),lit("|"),col("Id")),512).alias("payload_hash")
          ,col("_metadata.file_path").alias("source_file")
          ,current_timestamp().alias("processing_time")
          )    
 .writeStream
 .format("delta")
 .option("mergeSchema", "true")
 .option("checkpointLocation", checkpoint_path)
 .trigger(availableNow=True)
 .toTable(bronze_table)    
)
