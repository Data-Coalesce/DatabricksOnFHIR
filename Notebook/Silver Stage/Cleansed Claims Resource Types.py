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

# MAGIC %md
# MAGIC ##Step 1

# COMMAND ----------

from pyspark.sql.functions import explode,schema_of_json,lit,col,from_json,concat,collect_set,expr,size,array,struct,split
from pyspark.sql.types import DoubleType
df=(
     spark.read.table(bronze_source_table)
    .filter("resourceType='Claim'")
    .select(col("id").alias("payloadId"),"payload.status","payload.type","payload.patient","payload.billablePeriod","payload.created","payload.provider","payload.priority","payload.supportingInfo","payload.insurance","payload.item","payload.total")
    
     # explode out some detail so that we can roll them back up into a cleaner format
    .withColumn("encounter",explode(col("item.encounter"))) #break up the claim payload by encounters included in the claim
    .withColumn("claimLines",explode(col("item"))) #break up the payload even more into the individual lines
    .withColumn("claimProcedure",explode("claimLines.productOrService.coding")) #break out the procedures or services
    
     # these columns are qualified with type/id - don't really need the qualifier as it doesn't align with the referenced data which has the Id only
    .withColumn("patientId",split(col("patient.reference"),"/")[1]) 
    .withColumn("encounter",split(col("encounter.reference")[0],"/")[1])

    # FHIR has these nested within arrays, but there should only be one value for any individual payload
    .withColumn("claimType",col("type.coding.code")[0])   
    .withColumn("priority",col("priority.coding.code")[0])   
    
    # this produces a cleaner struct of the claim lines so they can be collected into a set in the next step
    .withColumn("claimLine",struct("claimLines.sequence","claimLines.net.value","claimProcedure.code","claimProcedure.display",concat("claimProcedure.system",lit("/"),"claimProcedure.code").alias("term")))   
    .withColumn("claimAmount",col("claimLines.net.value").cast(DoubleType()))
    
    .drop("patient","type","claimProcedure","claimLines","item")
)
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 2

# COMMAND ----------

from pyspark.sql.functions import array_sort,sum

dfRollup=(
    df.groupBy("payloadId","patientId","claimType","priority","status","created","billablePeriod")
    .agg(       
        sum(col("claimAmount")).alias("total"), # this is only being done here because sample data from SmartHealthIt has most claims total set to an arbitrary amount that is very repetitive.
        collect_set("encounter").alias("encounters"),
        collect_set("provider").alias("providers"),
        collect_set("supportingInfo").alias("supportingInfo"),
        array_sort(collect_set("claimLine")).alias("claimLines")     
        )
 )
display(dfRollup)
(
    dfRollup
    .write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .option("mergeSchema", "true")
    .saveAsTable(silver_table)
 )

