# Databricks notebook source
#variable declarations
bronze_source_table="fhir.bronze_all_resource_types"

# COMMAND ----------

# DBTITLE 1,Function for correcting schemas
def getSchema(df,columnName):
 value_nodes=df.select(col(columnName).alias('json')).rdd.map(lambda x: x.json)
 value_nodes.collect();
 value_schema=spark.read.json(value_nodes).schema
 return value_schema

# COMMAND ----------

# DBTITLE 1,Patients
from pyspark.sql.functions import explode,schema_of_json,lit,col,from_json,concat,collect_set,expr,size,array,struct,split
from pyspark.sql.types import DoubleType
df_patient=(
     spark.read.table(bronze_source_table)
    .filter("resource_type='Patient'")
    .select("payload.id",
            "payload.name",
            "payload.gender",
            col("payload.birthDate").alias("birth_date"),
            "payload.maritalStatus",
            "payload.telecom",
            "payload.communication",
            "payload.identifier",
            "payload.extension")
    .distinct()
    .withColumn("marital_status",col("maritalStatus")["coding"][0]["display"])
    .withColumn("first_name",expr("filter(name, x -> x.use == 'official')")[0]["given"][0])
    .withColumn("last_name",expr("filter(name, x -> x.use == 'official')")[0]["family"])
    .withColumn("maiden_name",expr("filter(name, x -> x.use == 'maiden')")[0]["family"])
    .withColumn("mrn",expr("filter(identifier, x -> x.type.coding[0].code == 'MR')")[0]["value"])
    .withColumn("mrn_source",expr("filter(identifier, x -> x.type.coding[0].code == 'MR')")[0]["system"])
    .withColumn("ssn",expr("filter(identifier, x -> x.type.coding[0].code == 'SS')")[0]["value"])   
    .withColumn("driver_license",expr("filter(identifier, x -> x.type.coding[0].code == 'DL')")[0]["value"])
    .withColumn("passport_number",expr("filter(identifier, x -> x.type.coding[0].code == 'PPN')")[0]["value"])    
    .withColumn("mother_maiden_name",expr("filter(extension, x -> x.url == 'http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName')")[0]["valueString"])
    .withColumn("birth_place",expr("filter(extension, x -> x.url == 'http://hl7.org/fhir/StructureDefinition/patient-birthPlace')")[0]["valueAddress"])
    .withColumn("language",col("communication.language.coding.display")[0])
    .withColumn("home_phone_number",expr("filter(telecom, x -> x.system == 'phone' and x.use='home')")["value"])
    .withColumn("work_phone_number",expr("filter(telecom, x -> x.system == 'phone' and x.use='work')")["value"])
    .withColumn("work_email",expr("filter(telecom, x -> x.system == 'email' and x.use='work')")["value"])
    .withColumn("medical_record",struct(col("mrn_source"),col("mrn")))
    .drop("identifier","extension","communication","name","telecom")
    .distinct()
     #this group by is a best guess at identifying unique patients across sources. Obviously not a very robust MDM method but it works for this demonstation
    .groupBy("last_name","ssn","driver_license","passport_number","gender","birth_date","birth_place")
    .agg(
        collect_set("id").alias("patient_source_ids"),
        collect_set("medical_record").alias("medical_records"),
        collect_set("first_name").alias("first_names"),
        collect_set("marital_status").alias("marital_status")
    )
)

# COMMAND ----------

# DBTITLE 1,Encounters
from pyspark.sql.functions import col,split,struct
from pyspark.sql.types import ArrayType,StructType,StructField,StringType

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

df_encounter=(
     spark.read.table(bronze_source_table)
    .filter("resource_type='Encounter'")
    .select("payload",
            "client",
            col("payload.type").alias("encounter_type"),
            col("payload.class").alias("encounter_class"),
            col("payload.id").alias("encounter_id"),            
            col("payload.serviceProvider").alias("service_provider"),
            "payload.status",
            "payload.period"
            )        
    .withColumn("encounter_type",from_json(col("encounter_type"),type_struct_fix))
    .withColumn("patient_id",split(col("payload.subject.reference"),"/")[1])
    .withColumn("reason_codes",explode(col("payload.reasonCode.coding")))
    .withColumn("participants",explode("payload.participant"))
    .drop("payload")
    .distinct()
)
#display(df_encounter)

# COMMAND ----------

# DBTITLE 1,Claims
from pyspark.sql.functions import explode,schema_of_json,lit,col,from_json,concat,collect_set,expr,size,array,struct,split,array_sort,sum
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

df_claim=(
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
   .groupBy(
        "payload_id",
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
     .withColumn("claim_struct",struct(
         col("claim_type"),
         col("priority"),
         col("status"),
         col("created"),
         col("billable_period"),
         col("claim_total_amount"),
         col("providers"),
         col("supporting_info"),
         col("claim_lines")
     ))
)
display(df_claim)

# COMMAND ----------

# DBTITLE 1,Procedures
from pyspark.sql.functions import col,split,struct,explode,collect_set
df_procedure=(
     spark.read.table(bronze_source_table)
    .filter("resource_type='Procedure'")
    .select("payload",
            "client",
            "payload.status",
            split(col("payload.subject.reference"),"/")[1].alias("patient_id"),
            split(col("payload.encounter.reference"),"/")[1].alias("encounter_id"),
            col("payload.id").alias("procedure_id"), 
            col("payload.reasonReference").alias("reason_references"),
            col("payload.performedPeriod").alias("performed_period")           
            )
    .withColumn("procedure_codes",explode(col("payload.code.coding")))
    .drop("payload")
    .distinct()
    .groupBy(
        "client",
        "procedure_id",
        "encounter_id",
        "patient_id",
        "status",
        "reason_references",
        "performed_period"
    )
    .agg(
        collect_set(col("procedure_codes")).alias("procedure_codes"))
    .withColumn("procedure_struct",
    struct(
        col("procedure_id"),
        col("status"),
        col("reason_references"),
        col("performed_period"),
        col("procedure_codes")
    ))
)
#display(df_procedure)

# COMMAND ----------

# DBTITLE 1,Conditions
from pyspark.sql.functions import col,split,struct,explode,collect_set
df_condition=(
     spark.read.table(bronze_source_table)
    .filter("resource_type='Condition'")
    .select("payload",
            "client",
            "payload.clinicalStatus",
      
            split(col("payload.subject.reference"),"/")[1].alias("patient_id"),
            split(col("payload.encounter.reference"),"/")[1].alias("encounter_id"),
            col("payload.id").alias("condition_id"), 
            col("payload.abatementDateTime").alias("abatement_timestamp"),
            col("payload.onsetDateTime").alias("onset_timestamp"),
            col("payload.recordedDate").alias("recorded_date")
            )
    .withColumn("verification_codes",explode(col("payload.verificationStatus.coding")))
    .withColumn("clinical_status_codes",explode(col("payload.clinicalStatus.coding")))
    .drop("payload")
    .distinct()
    .groupBy(
        "client",
        "condition_id",
        "encounter_id",
        "patient_id",
        "abatement_timestamp",
        "onset_timestamp",
        "recorded_date"
     )
    .agg(
        collect_set(col("verification_codes")).alias("verification_codes"),
        collect_set(col("clinical_status_codes")).alias("clinical_status_codes")
    )
    .withColumn("condition_struct",
    struct(
        col("condition_id"),
        col("abatement_timestamp"),
        col("onset_timestamp"),
        col("recorded_date"),
        col("verification_codes"),
        col("clinical_status_codes")
    ))
)
#display(df_condition)

# COMMAND ----------

# DBTITLE 1,Enrich encounters with sets of procedures and conditions
from pyspark.sql.functions import col,split,struct,count
encounterToProcedureJoinKeys=[
        df_encounter.client==df_procedure.client,
        df_encounter.patient_id==df_procedure.patient_id,
        df_encounter.encounter_id==df_procedure.encounter_id
       ]
encounterToConditionJoinKeys=[
df_encounter.client==df_condition.client,
df_encounter.patient_id==df_condition.patient_id,
df_encounter.encounter_id==df_condition.encounter_id
]
df_encounter_procedure=(
df_encounter
.join(
    df_procedure    
    ,encounterToProcedureJoinKeys,"left"
)
.join(df_condition,encounterToConditionJoinKeys,"left")
.select(
    df_encounter.patient_id,    
    df_encounter.encounter_id,
    df_encounter.encounter_class,
    df_encounter.encounter_type,
    df_encounter.status,
    df_encounter.service_provider,
    df_encounter.reason_codes,
    df_encounter.participants,
    df_encounter.period,
    df_procedure.procedure_struct,
    df_condition.condition_struct
    )
.distinct()
.groupBy(
    df_encounter.patient_id,
    df_encounter.encounter_id,
    df_encounter.encounter_class,
    df_encounter.encounter_type,
    df_encounter.status,
    df_encounter.service_provider,
    df_encounter.reason_codes,
    df_encounter.participants,
    df_encounter.period)
.agg(    
    collect_set("procedure_struct").alias("procedures"),
    collect_set("condition_struct").alias("conditions")
    )
 .withColumn("encounter_struct",struct(      
    df_encounter.encounter_id,
    df_encounter.encounter_class,
    df_encounter.encounter_type,
    df_encounter.status,
    df_encounter.service_provider,
    df_encounter.reason_codes,
    df_encounter.participants,
    df_encounter.period,
    col("procedures"),
    col("conditions")
     ))
)
#display(df_encounter_procedure)

# COMMAND ----------

# DBTITLE 1,Allergies
from pyspark.sql.functions import col,split,struct,explode,collect_set
df_allergy=(
     spark.read.table(bronze_source_table)
    .filter("resource_type='AllergyIntolerance'")
    .select("payload",
            "client",            
            "payload.type",
            "payload.category",
            "payload.criticality",  
            col("payload.code.text").alias("description"),
            split(col("payload.patient.reference"),"/")[1].alias("patient_id"),            
            col("payload.id").alias("allergy_id"), 
            col("payload.recordedDate").alias("recorded_date")          
            )
    .withColumn("allergy_codes",explode(col("payload.code.coding")))
    .withColumn("verification_codes",explode(col("payload.verificationStatus.coding")))
    .withColumn("clinical_status_codes",explode(col("payload.clinicalStatus.coding")))
    .drop("payload")
    .distinct()
    .groupBy(
        "client",              
        "patient_id",
        "type",
        "category",
        "criticality",
        "description",
        "recorded_date"
     )
    .agg(
        collect_set(col("allergy_id")).alias("allergy_source_ids"), #allergies are very repetive with different ID's even though payload content is the same
        collect_set(col("verification_codes")).alias("verification_codes"),
        collect_set(col("clinical_status_codes")).alias("clinical_status_codes"),
        collect_set(col("allergy_codes")).alias("allergy_codes")
        )
     .withColumn("allergy_struct",
     struct(         
         col("type"),
         col("category"),
         col("criticality"),
         col("description"),
         col("recorded_date"),
         col("verification_codes"),
         col("clinical_status_codes"),
         col("allergy_source_ids")
     ))
)
#display(df_allergy)

# COMMAND ----------

# DBTITLE 1,Immunizations
from pyspark.sql.functions import col,split,struct,explode
df_immunization=(
     spark.read.table(bronze_source_table)
    .filter("resource_type='Immunization'")
    .select("payload",
            "client",
            "payload.status",
             col("payload.occurrenceDateTime").alias("immunization_timestamp"),
             col("payload.id").alias("immunization_id"),
             split(col("payload.patient.reference"),"/")[1].alias("patient_id"),
             split(col("payload.encounter.reference"),"/")[1].alias("encounter_id")
    )
    .withColumn("vaccine_codes",explode(col("payload.vaccineCode.coding")))
    .drop("payload")
    .distinct()
    .groupBy(
        "client",       
        "patient_id",     
        "encounter_id",                    
        "immunization_timestamp",
        "status"
    )
    .agg(
        collect_set( "immunization_id").alias("immunization_source_ids"),
        collect_set("vaccine_codes").alias("vaccine_codes")
        )
      .withColumn("immunization_struct",
     struct(         
         col("immunization_timestamp"),
         col("vaccine_codes"),
         col("status"),
         col("immunization_source_ids")
  
     ))
)
#display(df_vaccine)

# COMMAND ----------

# DBTITLE 1,Enrich Patients with Encounters, Allergies, Immunization and Claims
from pyspark.sql.functions import col,split,struct,count,sha2,concat,row_number
from pyspark.sql.window import Window
w = Window().orderBy(lit(0))
df_patient_ids=(
    df_patient
    .withColumn("rollup_key",row_number().over(w)) #just need a basic row identifier so we can regroup after joining out the various source ids
    .withColumn("patient_id",explode("patient_source_ids"))
)
df_patient_joins=(
    df_patient_ids.join(
        df_encounter_procedure,
        df_patient_ids.patient_id == df_encounter_procedure.patient_id,
        "left"
        )
    .join(df_allergy,
          df_patient_ids.patient_id == df_allergy.patient_id,
          "left")
    .join(df_immunization,
        df_patient_ids.patient_id == df_immunization.patient_id,
        "left")
    .join(df_claim,
          df_patient_ids.patient_id == df_claim.patient_id,
          "left"
    )
    .select(
        df_patient_ids.rollup_key,
        df_encounter_procedure.encounter_struct,
        df_allergy.allergy_struct,
        df_immunization.immunization_struct,
        df_claim.claim_struct
    )
    .groupBy(df_patient_ids.rollup_key)
    .agg(
        collect_set("encounter_struct").alias("encounters"),
        collect_set("allergy_struct").alias("allergies"),
        collect_set("immunization_struct").alias("immunizations"),
        collect_set("claim_struct").alias("claims")
    )   
 )
df_patient_enriched=(
df_patient_ids.join(df_patient_joins,
                    df_patient_ids.rollup_key==df_patient_joins.rollup_key
                     ,"inner")
                     .drop(df_patient_ids.rollup_key,df_patient_joins.rollup_key)
 )
 

# COMMAND ----------

silver_table_fs="/user/hive/warehouse/fhir.db/silver/patient_enriched" #file system location for the table
silver_table_name="fhir.patient_enriched"
(
    df_patient_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .option("path",silver_table_fs)
    .saveAsTable(silver_table_name)   
)

