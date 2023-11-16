# Databricks notebook source
# MAGIC %md
# MAGIC #Load any FHIR resource type in new line delimited json (ndjson) format into the bronze layer
# MAGIC
# MAGIC All of the data used is sample data provided by https://bulk-data.smarthealthit.org/
# MAGIC
# MAGIC 🛈 This process could also handle regular json document arrays but you may need to use `.option("multiline", "true")
# MAGIC
# MAGIC 🛈 For this process to work as intended, it's required to have this config set `spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")`
# MAGIC
# MAGIC ⚠ While this notebook is setup to utilize autoloader, it currently uses a trigger now schedule. There are also reset commands below that will completely reset the checkpoints and target table
# MAGIC
# MAGIC ##Purpose
# MAGIC The idea is to load FHIR BULK EXPORT files, which should be in NDJSON format, into a single delta table regardless of the resource type. This particular loader, uses mounted cloud storage `source_path` which has subdirectories to simulate a multi-source(eg. client/customer) environment where the HL7 data is to be coalesced while maintaining the source information
# MAGIC
# MAGIC ##Issues
# MAGIC - 🤬 Each resource type has it's own json structure so letting spark infer the schema while loading mixed resource types results in errors
# MAGIC   - 🙂 **Solution**
# MAGIC     - 1. Load the ndjson files in format "text" so that spark doesn't try to infer the schema or convert the json into a struct
# MAGIC     - 2. Utilize micro-batching to apply custom function to each batch so that the text payload can be converted to a struct
# MAGIC     - 3. Within the same micro-batch, implement incremental logic to update the bronze table with the new payload struct
# MAGIC     - 4. Bronze table now has the original text payload, a struct payload, and some useful data from the payload to use for incremental upserts
# MAGIC
# MAGIC **This image shows the cloud storage contents. My proof of concept container is named "poc-blob" and I have a subdirectory for ndjson files. Any subdirectory within ndjson simulates a different source for the data.**
# MAGIC ![Cloud storage example](https://drive.google.com/uc?id=1yUpNgWe4ak2Btu6cYuRXUYMfjpZ-HdZT)
# MAGIC
# MAGIC ##References
# MAGIC https://customer-academy.databricks.com/learn/course/external/view/elearning/1800/data-engineering-with-databricks
# MAGIC
# MAGIC https://docs.databricks.com/en/structured-streaming/delta-lake.html
# MAGIC
# MAGIC https://smarthealthit.org/
# MAGIC
# MAGIC https://bulk-data.smarthealthit.org/

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true") # This is absolutely critical for this process to work as intended.  Otherwise the Merge in the micro batches can't evolve the schema in the payload struct column
# variable declarations
source_path="/mnt/fhir/ndjson/"
bronze_table="fhir.bronze_all_resource_types" #fully qualified table name
bronze_table_fs="/user/hive/warehouse/fhir.db/bronze_all_resource_types" #file system location for the table

checkpoint_path="/tmp/fhir/_checkpoint/all_resource_types" #checkpoint for autoloader

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠ The next cell will reset the checkpoint directory. Only do this if you want to reproces all of the files

# COMMAND ----------

dbutils.fs.rm(checkpoint_path,True) # only here only for resetting during dev/test

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠ The next cell will delete the bronze table and recreate it.  Obviously don't do that if you want to incrementally load new data as it shows up

# COMMAND ----------

from delta.tables import *
spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")
dbutils.fs.rm(bronze_table_fs,True) #make sure the file system is clean
spark.sql(f"""CREATE TABLE IF NOT EXISTS {bronze_table}
          (          
              client string,
              payload_id string,
              resource_type string,
              processing_time timestamp NOT NULL,
              value string NOT NULL,
              source_file string NOT NULL,
              payload struct<client_id string,payload_id string,resource_type string>         
          )          
          USING DELTA
          LOCATION "{bronze_table_fs}"
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC Function `valueSchema` accepts a dataframe from the microbatch and maps the "value" column (which should be StringType representation of valid json) to an RDD.  The RDD's are then collected and the schema 

# COMMAND ----------

def valueSchema(df,colName):
 value_nodes=df.select(col(colName).alias('json')).rdd.map(lambda x: x.json)
 value_nodes.collect();
 value_schema=spark.read.json(value_nodes).schema
 return value_schema

# COMMAND ----------

from pyspark.sql.functions import from_json

def upsertToDelta(microBatchOutputDF, batchId):
  payload_schema=valueSchema(microBatchOutputDF,"value");
  microBatchOutputDF=(
        microBatchOutputDF.select("value","client","source_file","processing_time",from_json("value",schema=payload_schema).alias("payload"))
       .withColumn("resource_type",col("payload.resourceType"))
       .withColumn("payload_id",col("payload.id"))
  )
  microBatchOutputDF.createOrReplaceTempView("payloads")  

  microBatchOutputDF.sparkSession.sql(f"""
    MERGE INTO {bronze_table} AS t
    USING payloads AS s
    ON s.client = t.client
        AND s.payload_id=t.payload_id
        AND s.resource_type=t.resource_type
    WHEN MATCHED THEN 
        UPDATE SET             
            processing_time=s.processing_time,
            source_file=s.source_file,
            payload=s.payload
    WHEN NOT MATCHED THEN INSERT (client,payload_id,resource_type,source_file,processing_time,value,payload) VALUES(s.client,s.payload_id,s.resource_type,s.source_file,s.processing_time,s.value,s.payload)
  """)

# COMMAND ----------

from pyspark.sql.functions import col,regexp_replace,concat,lit,current_timestamp,unix_timestamp
from pyspark.sql import SparkSession
spark = SparkSession.builder \
               .appName('FHIR Bronze Load') \
               .getOrCreate()

# next 4 lines simply persist the current timestamp as a constant so we can reference it in the next step to determine which stream writes we need to process
df_ts=spark.createDataFrame([["current_timestamp"]],["variable"])
df_ts.withColumn("value",current_timestamp()).show() #force evaluation so we have a timestamp marker to use in next steps
ts=df_ts.select("current_timestamp").first()[0]
stream_start_ts = spark.sparkContext.broadcast(ts)

stream_df=(spark.readStream
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
  .outputMode("update")
  .option("checkpointLocation", checkpoint_path)  
  .trigger(availableNow=True)      
  .foreachBatch(upsertToDelta)
  .start()
)
display(stream_df)
