# Databricks notebook source
# MAGIC %md
# MAGIC # Patient Silver Tier
# MAGIC This notebook extracts "patient" resource types from the bronze staging table. The purpose is to select only the relevant columns and normalize it to a degree by cleaning up deep structs and messy arrays.
# MAGIC
# MAGIC #### Step 1
# MAGIC  1. Read the bronze table and filter for patients
# MAGIC  2. Select columns relevant and populated for patients
# MAGIC  3. Explode columns containing arrays of structs
# MAGIC  4. Simplify deep arrays
# MAGIC  5. Extract out nested identifiers such as SSN, MRN, DL etc.
# MAGIC  6. Drop columns no longer needed
# MAGIC
# MAGIC #### Step 2
# MAGIC  1. Regroup the patient by identifiers
# MAGIC  2. Rollup sets of identifiers that could exist across sources 
# MAGIC  3. Write out to Silver Delta table
# MAGIC

# COMMAND ----------

#variable declarations
bronze_source_table="fhir.bronze_all_resource_types"
silver_table="fhir.silver_patient"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1
# MAGIC Read in the resource type from the bronze table and explode out struct columns and cleanse other columns as needed

# COMMAND ----------

from pyspark.sql.functions import explode,schema_of_json,lit,col,from_json,concat,collect_set,expr,size,array,struct,split
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import explode,schema_of_json,lit,col,from_json,concat,collect_set,expr,size,array,struct

df=(
     spark.read.table(bronze_source_table)
    .filter("resource_type='Patient'")
    .select("payload.name","payload.gender","payload.birthDate","payload.maritalStatus","payload.telecom","payload.communication","payload.identifier","payload.extension")
    .distinct()
    .withColumn("maritalStatus",col("maritalStatus")["coding"][0]["display"])
    .withColumn("firstName",expr("filter(name, x -> x.use == 'official')")[0]["given"][0])
    .withColumn("lastName",expr("filter(name, x -> x.use == 'official')")[0]["family"])
    .withColumn("maidenName",expr("filter(name, x -> x.use == 'maiden')")[0]["family"])
    .withColumn("mrn",expr("filter(identifier, x -> x.type.coding[0].code == 'MR')")[0]["value"])
    .withColumn("mrn_source",expr("filter(identifier, x -> x.type.coding[0].code == 'MR')")[0]["system"])
    .withColumn("ssn",expr("filter(identifier, x -> x.type.coding[0].code == 'SS')")[0]["value"])   
    .withColumn("driverLicense",expr("filter(identifier, x -> x.type.coding[0].code == 'DL')")[0]["value"])
    .withColumn("passportNumber",expr("filter(identifier, x -> x.type.coding[0].code == 'PPN')")[0]["value"])    
    .withColumn("motherMaidenName",expr("filter(extension, x -> x.url == 'http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName')")[0]["valueString"])
    .withColumn("birthPlace",expr("filter(extension, x -> x.url == 'http://hl7.org/fhir/StructureDefinition/patient-birthPlace')")[0]["valueAddress"])
    .withColumn("language",col("communication.language.coding.display")[0])
    .withColumn("homePhoneNumber",expr("filter(telecom, x -> x.system == 'phone' and x.use='home')")["value"])
    .withColumn("workPhoneNumber",expr("filter(telecom, x -> x.system == 'phone' and x.use='work')")["value"])
    .withColumn("workEmail",expr("filter(telecom, x -> x.system == 'email' and x.use='work')")["value"])
    .withColumn("medicalRecord",struct(col("mrn_source"),col("mrn")))
    .drop("identifier","extension","communication","name","telecom")
    .distinct()
)
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 2
# MAGIC Summarize the dataframe by aggregating numerics and collecting cleansed sets from previously exploded columns, then write to silver table destination
# MAGIC For patients, the idea is to collect medical records(MRN) from any source and attribute to the same patient.
# MAGIC
# MAGIC Additionaly, account for variances across sources for name spelling and demographical info etc. by storing in an array rather than replacing

# COMMAND ----------

df_rollup=(
df.groupBy("lastName","ssn","driverLicense","passportNumber","gender","birthDate","birthPlace").agg(
collect_set("medicalRecord").alias("medicalRecords"),
collect_set("firstName").alias("firstNames"),
collect_set("maritalStatus").alias("maritalStatus")
)

)
display(df_rollup)
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable("silver_patients")
