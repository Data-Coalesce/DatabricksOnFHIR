# Databricks notebook source
#variable declarations
source_path="/mnt/fhir/ndjson/"
bronze_table="fhir.bronze_all_resource_types"
bronze_table_fs="/user/hive/warehouse/fhir.db/bronze_all_resource_types"

checkpoint_path="/tmp/fhir/_checkpoint/all_resource_types2"
dbutils.fs.ls(source_path)
dbutils.fs.rm(bronze_table_fs,True)

dbutils.fs.ls(source_path)

# COMMAND ----------

dbutils.fs.rm(checkpoint_path,True) # only here only for resetting during dev/test
dbutils.fs.ls(checkpoint_path)

# COMMAND ----------

from delta.tables import *

spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {bronze_table}
          (
              value string NOT NULL,
              source_file string NOT NULL,
              client string,
              payloadId string,
              resourceType string,
              processing_time timestamp NOT NULL,
              payload struct<>               
          )          
          USING DELTA
          LOCATION "{bronze_table_fs}"
          """)
#delta_bronze_table=DeltaTable.forPath(spark,bronze_table_fs) #define a DeltaTable reference for our Merge

# COMMAND ----------

def valueSchema(df):
 value_nodes=df.select(col("value").alias('j')).rdd.map(lambda x: x.j)
 value_nodes.collect();
 value_schema=spark.read.json(value_nodes).schema
 return value_schema

# COMMAND ----------

from pyspark.sql.functions import from_json

def upsertToDelta(microBatchOutputDF, batchId):

  payload_schema=valueSchema(microBatchOutputDF);
  microBatchOutputDF=(
        microBatchOutputDF.select("value","client","source_file","processing_time",from_json("value",schema=payload_schema).alias("payload"))
       .withColumn("resourceType",col("payload.resourceType"))
       .withColumn("id",col("payload.id"))
  )
  microBatchOutputDF.createOrReplaceTempView("payloads")  

  microBatchOutputDF.sparkSession.sql(f"""
    MERGE INTO {bronze_table} AS t
    USING payloads AS s
    ON s.client = t.client
        AND s.id=t.payloadId
        AND s.resourceType=t.resourceType
    WHEN MATCHED and t.value<>s.value THEN 
        UPDATE SET 
            payload=s.payload, 
            processing_time=s.processing_time,
            source_file=s.source_file
    WHEN NOT MATCHED THEN INSERT *
  """)

# COMMAND ----------

from pyspark.sql.functions import col,regexp_replace,concat,lit,current_timestamp

bronze_df=(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "text")
  .option("recursiveFileLookup","true")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .option("inferSchema","false")
  .load(source_path)
  .select("*"
          ,regexp_replace(regexp_replace("_metadata.file_path",source_path,""),concat(lit("/"),col("_metadata.file_name")),"").alias("client")          
          ,col("_metadata.file_path").alias("source_file")
          ,current_timestamp().alias("processing_time")
          )
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .foreachBatch(upsertToDelta)
  .outputMode("update")
  .start()
)
display(bronze_df)


# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.databricks.com/en/structured-streaming/delta-lake.html

# COMMAND ----------


