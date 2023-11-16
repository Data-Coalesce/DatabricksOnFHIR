# Databricks notebook source
#variable declarations
bronze_source_table="fhir.bronze_all_resource_types"
silver_table="fhir.silver_claims"

# COMMAND ----------

# DBTITLE 1,Patients
from pyspark.sql.functions import explode,schema_of_json,lit,col,from_json,concat,collect_set,expr,size,array,struct,split
from pyspark.sql.types import DoubleType
df_patient=(
     spark.read.table(bronze_source_table)
    .filter("resource_type='Patient'")
    .select("payload.id","payload.name","payload.gender","payload.birthDate","payload.maritalStatus","payload.telecom","payload.communication","payload.identifier","payload.extension")
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
    .groupBy("lastName","ssn","driverLicense","passportNumber","gender","birthDate","birthPlace")
    .agg(
        collect_set("id").alias("patient_source_ids"),
        collect_set("medicalRecord").alias("medicalRecords"),
        collect_set("firstName").alias("firstNames"),
        collect_set("maritalStatus").alias("maritalStatus")
    )
)


# COMMAND ----------

# DBTITLE 1,Encounters
from pyspark.sql.functions import col,split,struct
df_encounter=(
     spark.read.table(bronze_source_table)
    .filter("resource_type='Encounter'")
    .select("payload",
            "client",
            col("payload.id").alias("encounter_id"),
            col("payload.serviceProvider").alias("service_provider"),
            "payload.status",
            "payload.period"
            )
    .withColumn("patient_id",split(col("payload.subject.reference"),"/")[1])
    .withColumn("reason_codes",explode(col("payload.reasonCode.coding")))
    .drop("payload")
    .distinct()
)
#display(df_encounter)

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
    df_procedure.procedure_struct,
    df_condition.condition_struct
    )
.groupBy(
    df_encounter.patient_id,
    df_encounter.encounter_id)
.agg(    
    collect_set("procedure_struct").alias("procedures"),
    collect_set("condition_struct").alias("conditions")
    )
 .withColumn("encounter_struct",struct(df_encounter.encounter_id,col("procedures"),col("conditions")))
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
        "allergy_id",        
        "patient_id",
        "type",
        "category",
        "criticality",
        "recorded_date"
     )
    .agg(
        collect_set(col("verification_codes")).alias("verification_codes"),
        collect_set(col("clinical_status_codes")).alias("clinical_status_codes"),
        collect_set(col("allergy_codes")).alias("allergy_codes")
        )
    
     .withColumn("condition_struct",
     struct(
         col("allergy_id"),
         col("type"),
         col("category"),
         col("criticality"),
         col("recorded_date"),
         col("verification_codes"),
         col("clinical_status_codes")
     ))
)
#display(df_allergy)

# COMMAND ----------

# DBTITLE 1,Immunizations
from pyspark.sql.functions import col,split,struct,explode
df_vaccine=(
     spark.read.table(bronze_source_table)
    .filter("resource_type='Immunization'")
    .select("payload",
            "client",
            "payload.status",
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
        "immunization_id",
        "status"
    )
    .agg(collect_set("vaccine_codes").alias("vaccine_codes"))
      .withColumn("immunization_struct",
     struct(
         col("immunization_id"),
         col("vaccine_codes"),
         col("status")
  
     ))
)
#display(df_vaccine)

# COMMAND ----------

# DBTITLE 1,Enrich Patients with Encounters, Allergies, Immunization
from pyspark.sql.functions import col,split,struct,count,sha2,concat,row_number
from pyspark.sql.window import Window
w = Window().orderBy(lit(0))
df_patient_ids=(
    df_patient
    .withColumn("rollup_key",row_number().over(w)) #just need a basic row identifier so we can regroup after joining out the various source ids
    .withColumn("patient_id",explode("patient_source_ids"))
)
df_patient_enriched=(
    df_patient_ids.join(
        df_encounter_procedure,df_patient_ids.patient_id == df_encounter_procedure.patient_id,
        "left"
        )
    .select(
        df_patient_ids,
        df_encounter_procedure.encounter_struct
    )
 )
display(df_patient_enriched)
#display(df_patient_encounter)
# patientToEncounterJoinKeys=[    
#          df_patient_ids.patient_id == df_encounter.patient_id      
#  ]
        
#        ]
# encounterToConditionJoinKeys=[
# df_encounter.client==df_condition.client,
# df_encounter.patient_id==df_condition.patient_id,
# df_encounter.encounter_id==df_condition.encounter_id
# ]
