# Databricks notebook source
# DBTITLE 1,Basic UDF to bucketize age groups
@udf
def age_bucket(age):
  if age < 18:
    return "Minor"
  elif age < 65:
    return "Adult"  
  else: 
    return "Senior"

# COMMAND ----------

# DBTITLE 1,Create base dataframe with some new columns for analysis
from pyspark.sql.functions import months_between,current_date,col,lit,floor,count
silver_table_name="fhir.patient_enriched"
df_patient=(
    spark
    .read
    .table(silver_table_name)    
    )
df_patient=(
    df_patient
    .withColumn("age",floor(months_between(current_date(),col("birthDate"))/lit(12)))
    .withColumn("age_group",age_bucket(col("age")))
    .withColumn("marital_status",col("maritalStatus")[0])
)

# COMMAND ----------

# DBTITLE 1,Representation by Age, Gender and Marital Status
from pyspark.sql.functions import months_between,current_date,col,lit,floor,count
df_demo=(
df_patient
.select("*")
.drop("birthDate") 
.groupBy("gender","age_group","age","marital_status")
.agg(
    count("*").alias("patient_count")
     )
)
display(df_demo)

# COMMAND ----------

from pyspark.sql.functions import col,explode,concat,lit,date_format,datepart
df_demo=(
df_patient
.select("age","age_group","gender","marital_status","immunizations")
.withColumn("immunization",explode("immunizations"))
.withColumn("status",col("immunization.status"))
.withColumn("vaccine_name",col("immunization.vaccine_codes.display")[0])
.withColumn("vaccine_standard",concat(col("immunization.vaccine_codes.system")[0],lit("/"),col("immunization.vaccine_codes.code")[0]))
.withColumn("vaccination_date",date_format(col("immunization.immunization_timestamp"),"yyyy-MM-dd"))
.withColumn("vaccination_YYYYMM",date_format( col("vaccination_date"),"yyyy-MM"))
.drop("immunizations","immunization")
 .groupBy("gender",
          "age",
          "age_group",
          "marital_status",
          "status",
          "vaccine_name",
          "vaccine_standard",
          col("vaccination_YYYYMM").alias("vaccination_period")
          )
 .agg(
     count("*").alias("patient_count")
      )
)
display(df_demo)
