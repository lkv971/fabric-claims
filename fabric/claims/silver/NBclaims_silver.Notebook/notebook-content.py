# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b98cd8e8-9a67-459f-9eae-c4311796500b",
# META       "default_lakehouse_name": "LHclaims_silver",
# META       "default_lakehouse_workspace_id": "d6cf1ffb-bc19-4130-bfa1-27701036ae71",
# META       "known_lakehouses": [
# META         {
# META           "id": "b98cd8e8-9a67-459f-9eae-c4311796500b"
# META         },
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
import notebookutils
import json
import uuid
import builtins

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_id = str(uuid.uuid4())
run_ts = datetime.now(ZoneInfo("America/New_York"))
pii_salt = "healthcare_secure_salt_2024" 
bronze_tables_root = "abfss://Claims@onelake.dfs.fabric.microsoft.com/LHclaims_bronze.Lakehouse/Tables"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_root = bronze_tables_root.rstrip("/")
path_claims_clean = f"{bronze_root}/claims_clean"

if not notebookutils.fs.exists(path_claims_clean):
    raise FileNotFoundError(f"Delta path not found: {path_claims_clean}")

df_quality = (
    spark.read
         .format("delta")
         .load(path_claims_clean)
         .cache()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_patients = df_quality.select("PatientID", 
"PatientAge",
"PatientGender",
"PatientMaritalStatus",
"PatientEmploymentStatus").distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_providers = df_quality.select("ProviderID",
 "ProviderSpecialty",
 "ProviderLocation").distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_claims = df_quality.select("ClaimID",
"PatientID",
"ProviderID",
"ClaimDate",
"ClaimAmount",
"DiagnosisCode",
"ProcedureCode",
"ClaimStatus",
"ClaimType",
"ClaimSubmissionMethod")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_date = "20150101"
end_date   = "20301231"

df_date_range = spark.createDataFrame([(start_date, end_date)], ["start_date", "end_date"])
df_dates = df_date_range.select(
    explode(sequence(to_date(col("start_date"), "yyyyMMdd"),
                     to_date(col("end_date"), "yyyyMMdd"),
                     expr("interval 1 day"))).alias("Date"))

window_dates = Window.orderBy("Date")

df_dates = df_dates.withColumn("Year", year(col("Date"))) \
                   .withColumn("DateID", row_number().over(window_dates)) \
                   .withColumn("Month", month(col("Date"))) \
                   .withColumn("Day", dayofmonth(col("Date"))) \
                   .withColumn("MonthName", date_format(col("Date"), "MMMM")) \
                   .withColumn("DayName", date_format(col("Date"), "EEEE")) \
                   .select("DateID", "Date", "Year", "Month", "Day", "MonthName", "DayName") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_patients = df_patients.withColumn("PatientID", sha2(concat(col("PatientID"), lit(pii_salt)), 256))

df_providers = df_providers.withColumn("ProviderID", sha2(concat(col("ProviderID"), lit(pii_salt)), 256))

df_claims = df_claims.withColumn("PatientID", sha2(concat(col("PatientID"), lit(pii_salt)), 256)) \
                     .withColumn("ProviderID", sha2(concat(col("ProviderID"), lit(pii_salt)), 256))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_patients = df_patients.withColumn("CreatedDate", lit(run_ts)) \
                         .withColumn("ModifiedDate", lit(run_ts)) \
                         .withColumn("ProcessBatchID", lit(run_id))

df_providers = df_providers.withColumn("CreatedDate", lit(run_ts)) \
                           .withColumn("ModifiedDate", lit(run_ts)) \
                           .withColumn("ProcessBatchID", lit(run_id))

df_claims = df_claims.withColumn("CreatedDate", lit(run_ts)) \
                     .withColumn("ModifiedDate", lit(run_ts)) \
                     .withColumn("ProcessBatchID", lit(run_id))

df_dates = df_dates.withColumn("CreatedDate", lit(run_ts)) \
                   .withColumn("ModifiedDate", lit(run_ts)) \
                   .withColumn("ProcessBatchID", lit(run_id))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_patients = df_patients.dropDuplicates(["PatientID"])
df_providers = df_providers.dropDuplicates(["ProviderID"])
df_claims = df_claims.dropDuplicates(["ClaimID"])
df_dates = df_dates.dropDuplicates(["DateID"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def overwrite_table(df, table_name):
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(table_name))
    print(f"Overwrote {table_name}")

overwrite_table(df_dates, "dim_dates")
overwrite_table(df_patients, "dim_patients")
overwrite_table(df_providers, "dim_providers")


if spark.catalog.tableExists("fact_claims"):
    target = DeltaTable.forName(spark, "fact_claims")
    (target.alias("t")
        .merge(df_claims.alias("s"), "t.ClaimID = s.ClaimID")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print("Upserted fact_claims.")
else:
    (df_claims.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("fact_claims"))
    print("Created fact_claims.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows_processed = {
    "dim_dates": df_dates.count(),
    "dim_patients": df_patients.count(),
    "dim_providers": df_providers.count(),
    "fact_claims": df_claims.count()
}

total_rows_processed = builtins.sum(rows_processed.values())


print(f"Total rows processed (silver): {total_rows_processed}")

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
    "total_rows_processed": total_rows_processed
}

mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
