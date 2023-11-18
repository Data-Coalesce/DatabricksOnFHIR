# Databricks notebook source
#variable declarations
bronze_source_table="fhir.bronze_all_resource_types"
silver_table_fs="/user/hive/warehouse/fhir.db/silver/provider_enriched" #file system location for the table
silver_table_name="fhir.provider_enriched"

# COMMAND ----------

from pyspark.sql.functions import explode,schema_of_json,lit,col,from_json,concat,collect_set,expr,size,array,struct,split
from pyspark.sql.types import DoubleType
df_providers=(
     spark.read.table(bronze_source_table)
    .filter("resource_type='Practitioner'")
    .select("payload_id",
            "client",
            col("payload.id").alias("provider_id"),
            "payload.extension",
            "payload.address",
            "payload.identifier",
            "payload.name",
            "payload.telecom"
            )
     .distinct()
     .withColumn("npi", expr("filter(identifier, x -> x.system == 'http://hl7.org/fhir/sid/us-npi')")[0]["value"])
     .withColumn("source_id_key_pair",struct(col("client"),col("provider_id")))
         .groupBy("npi").agg(
      collect_set("source_id_key_pair").alias("source_id_key_pair"),
      collect_set("telecom").alias("contactInfo"),
      collect_set("identifier").alias("identifier"),
      collect_set("address").alias("address"),
      collect_set("name").alias("name"),
      )
)
display(df_providers)
