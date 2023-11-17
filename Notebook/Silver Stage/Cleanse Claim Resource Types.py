# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Silver Tier
# MAGIC This notebook extracts "claim" resource types from the bronze staging table. The purpose is to select only the relevant columns and normalize it to a degree by cleaning up deep structs and messy arrays.
# MAGIC
# MAGIC ## Step 1
# MAGIC  1. Read the bronze table and filter for claims
# MAGIC  2. Select columns relevant and populated for claims
# MAGIC  3. Explode columns containing arrays of structs
# MAGIC  4. Cleanse data by reducing redundant arrays and stripping foreign key prefixes e.g. "*Encounter/*[id]"
# MAGIC  5. Extract claim line amounts so they can be aggregated *I am only doing this because the sample data being used has the same "Total" amount repeated for most of the claims.  To make the demo data look more legit, I am replacing the "Total" with the sum of the line values*
# MAGIC  6. Restructure claim line items into a cleaner array of structs
# MAGIC  7. Drop columns no longer needed
# MAGIC
# MAGIC ## Step 2
# MAGIC  1. Regroup the claim payload
# MAGIC  2. Aggregate SUM of claim line amount as new claim "total" *again, only being done to improve demo data*
# MAGIC  3. Collect exploded columns into cleaner arrays of structs
# MAGIC  4. Write out to Silver Delta table
# MAGIC

# COMMAND ----------

#variable declarations
bronze_source_table="fhir.bronze_all_resource_types"
silver_table="fhir.silver_claims"

# COMMAND ----------

# DBTITLE 1,Step 1
from pyspark.sql.functions import explode,schema_of_json,lit,col,from_json,concat,collect_set,expr,size,array,struct,split
from pyspark.sql.types import DoubleType,StructType,ArrayType,StructField,StringType

# the type struct in the payload doesn't evolve correctly because of mixed schemas, so it gets stored as a string and we have to format it manually per resource type
type_struct_fix=ArrayType(StructType([
    StructField('coding', 
                 ArrayType(StructType([
                     StructField('code', StringType(), True), 
                     StructField('display', StringType(), True), 
                     StructField('system', StringType(), True)])
                           , True)
                 , True), 
     StructField('text', StringType(),True)
     ]),True)

df=(
     spark.read.table(bronze_source_table)
    .filter("resource_type='Claim'")
    .select(
        "payload",
        "payload.payload_id",
        "payload.type",
        "payload.status",
        "payload.patient",
        "payload.billablePeriod",
        "payload.created",
        "payload.provider",
        "payload.priority",
        "payload.supportingInfo",
        "payload.insurance",        
        "payload.total"
        )
    
     # explode out some detail so that we can roll them back up into a cleaner format
    
    .withColumn("claim_line",explode(col("payload.item"))) #break up the payload even more into the individual lines   
    .withColumn("claim_procedure",explode("claim_line.productOrService.coding")) #break out the procedures or services
    
     # these columns are qualified with type/id - don't really need the qualifier as it doesn't align with the referenced data which has the Id only
    .withColumn("patient_id",split(col("patient.reference"),"/")[1]) 
    .withColumn("claim_line_encounter_id",split(col("claim_line.encounter.reference")[0],"/")[1])

    # FHIR has these nested within arrays, but there should only be one value for any individual payload
    .withColumn("claim_type", from_json(col("type"),type_struct_fix).alias("claim_type"))
    .withColumn("priority",col("priority.coding.code")[0])   
    .withColumn("claim_line_amount",col("claim_line.net.value").cast(DoubleType()))
    # this produces a cleaner struct of the claim lines so they can be collected into a set in the next step
    .withColumn("claim_line",struct(
        "claim_line.sequence",
        "claim_line_amount",
        "claim_procedure.code",
        "claim_procedure.display",
        "claim_line_encounter_id",
        concat("claim_procedure.system",lit("/"),
               "claim_procedure.code").alias("term")
        )
    )   
   
   .drop("payload","patient","type","claim_procedure","item")
)
display(df)


# COMMAND ----------

# DBTITLE 1,Step 2
from pyspark.sql.functions import array_sort,sum

df_rollup=(
    df.groupBy("payload_id",
               "patient_id",
               "claim_type",               
               "priority",
               "status",
               "created",
               col("billablePeriod").alias("billable_period")
               )
    .agg(       
        sum(col("claim_line_amount")).alias("claim_total_amount"), # this is only being done here because sample data from SmartHealthIt has most claims total set to an arbitrary amount that is very repetitive.
        collect_set("provider").alias("providers"),
        collect_set("supportingInfo").alias("supporting_info"),
        array_sort(collect_set("claim_line")).alias("claim_lines")     
        )
 )
display(df_rollup)
(
    df_rollup
    .write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .option("mergeSchema", "true")
    .saveAsTable(silver_table)
 )

