# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "af5da6f5-d43f-46c3-8924-7e10fdeaca18",
# META       "default_lakehouse_name": "LHclaims_bronze",
# META       "default_lakehouse_workspace_id": "d6cf1ffb-bc19-4130-bfa1-27701036ae71",
# META       "known_lakehouses": [
# META         {
# META           "id": "af5da6f5-d43f-46c3-8924-7e10fdeaca18"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timezone
from zoneinfo import ZoneInfo 
from notebookutils import mssparkutils
import json
import uuid
import builtins

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_path = "Files/raw_claims/data_02921ab2-89f8-4d0f-9354-e0861d871f8d_c9ca6213-2671-45dc-b86f-3bbd00c06cf5.txt"
run_id = str(uuid.uuid4())
run_ts = datetime.now(ZoneInfo("America/New_York"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_schema = StructType([
    StructField("ClaimID", StringType(), False),
    StructField("PatientID", StringType(), False),
    StructField("ProviderID", StringType(), False),
    StructField("ClaimAmount", DecimalType(18,2), True),
    StructField("ClaimDate", DateType(), True),
    StructField("DiagnosisCode", StringType(), True),
    StructField("ProcedureCode", StringType(), True),
    StructField("PatientAge", IntegerType(), True),
    StructField("PatientGender", StringType(), True),
    StructField("ProviderSpecialty", StringType(), True),
    StructField("ClaimStatus", StringType(), True),
    StructField("PatientIncome", DecimalType(18,2), True),
    StructField("PatientMaritalStatus", StringType(), True),
    StructField("PatientEmploymentStatus", StringType(), True),
    StructField("ProviderLocation", StringType(), True),
    StructField("ClaimType", StringType(), True),
    StructField("ClaimSubmissionMethod", StringType(), True),
    StructField("CorruptRecord", StringType(), True)
])

raw_path = "Files/raw_claims/data_02921ab2-89f8-4d0f-9354-e0861d871f8d_c9ca6213-2671-45dc-b86f-3bbd00c06cf5.txt"

df_raw = spark.read.csv(raw_path, schema = raw_schema, header = True, sep =",", mode = "PERMISSIVE", columnNameOfCorruptRecord = "CorruptRecord")

df_raw = df_raw.withColumn("IngestedAt", current_timestamp()) \
               .withColumn("SourceFile", input_file_name()) \
               .withColumn("ProcessBatchID", expr("uuid()")) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_malformed = df_raw.filter(col("CorruptRecord").isNotNull()) 
df_malformed = df_malformed.withColumn("RetentionDate", date_add(current_date(), 90))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_clean = df_raw.filter(col("CorruptRecord").isNull())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_duplicates = Window.partitionBy("ClaimID").orderBy(col("ClaimDate").desc())

df_with_duplicates = df_clean.withColumn("row_num", row_number().over(window_duplicates))

df_unique = df_with_duplicates.filter(col("row_num") == 1 ).drop(col("row_num"))

df_duplicates = df_with_duplicates.filter(col("row_num") > 1).drop(col("row_num")) \
                                  .withColumn("RetentionDate", date_add(current_date(), 180))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

valid_genders = ["F", "M", "U", "Other"]
valid_status = ["Approved", "Denied", "Pending", "Partial"]
valid_types = ["Routine", "Emergency", "Inpatient", "Outpatient", "Urgent Care"]
valid_submission = ["Paper", "Online", "Phone"]
uuid_regex =  r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dq_conditions = [
    (col("ClaimAmount") <= 0, "InvalidClaimAmount"),
    ((col("PatientAge") < 0) | (col("PatientAge") > 120), "Invalid PatientAge"),
    (~col("PatientGender").isin(valid_genders), "Invalid PatientGender"),
    (~col("ClaimStatus").isin(valid_status), "Invalid ClaimStatus"),
    (~col("ClaimType").isin(valid_types), "Invalid ClaimType"),
    (~col("ClaimSubmissionMethod").isin(valid_submission), "Invalid ClaimSubmission"),
    (~col("ClaimID").rlike(uuid_regex), "Invalid ClaimID format"),
    (~col("PatientID").rlike(uuid_regex), "Invalid PatientID format"),
    (~col("ProviderID").rlike(uuid_regex), "Invalid ProviderID format")
]

dq_reasons = array([when(condition, lit(reason)).otherwise(lit(None)) for condition, reason in dq_conditions])

df_bad_data = df_unique.withColumn("DataQualityReason", dq_reasons)
df_bad_data = df_bad_data.filter(exists(col("DataQualityReason"), lambda x:x.isNotNull())) \
                         .withColumn("DataQualityReasonString", concat_ws(";", col("DataQualityReason"))) \
                         .drop("DataQualityReason") \
                         .withColumnRenamed("DataQualityReasonString", "DataQualityReason") \
                         .withColumn("RetentionDate", date_add(current_date(), 365))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_quality = df_unique.join(df_bad_data.select("ClaimID"), on="ClaimID", how="left_anti")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_malformed = df_malformed.cache()
df_duplicates = df_duplicates.cache()
df_bad_data = df_bad_data.cache()
df_quality = df_quality.cache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

malformed_count = df_malformed.count()
duplicates_count = df_duplicates.count()
bad_data_count = df_bad_data.count()
quality_count = df_quality.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_malformed.write.format("delta").mode("append").saveAsTable("malformed_claims")
df_duplicates.write.format("delta").mode("append").saveAsTable("dupe_claims")
df_bad_data.write.format("delta").mode("append").saveAsTable("data_quality_claims")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists("claims_clean"):
    target = DeltaTable.forName(spark, "claims_clean")
    (target.alias("t")
        .merge(df_quality.alias("s"), "t.ClaimID = s.ClaimID")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print("Upserted Bronze claims_clean (by ClaimID).")
else:
    (df_quality
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("claims_clean"))
    print("Created Bronze claims_clean.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows_processed = {"claims_clean": quality_count}
quality_metrics = {
    "malformed_records": malformed_count,
    "duplicate_records": duplicates_count,
    "bad_quality_records": bad_data_count,
    "valid_records": quality_count
}

print(f"Quality issues: {builtins.sum(quality_metrics.values()) - quality_metrics['valid_records']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = {
    "status": "succeeded",
    "run_id": run_id,
    "run_ts": run_ts.isoformat(),
    "rows_processed": rows_processed,
    "quality_metrics": quality_metrics
}

mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
