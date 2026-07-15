# Databricks notebook source
patients = spark.read.option("header", True).csv("/Volumes/workspace/default/health_care/patients.csv")
conditions = spark.read.option("header", True).csv("/Volumes/workspace/default/health_care/conditions.csv")
encounters = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/workspace/default/health_care/encounters.csv")

display(patients)

display(conditions)

display(encounters)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window = Window.partitionBy("PATIENT") \
               .orderBy(col("START").desc())

latest_condition = conditions.withColumn(
    "rn",
    row_number().over(window)
).filter(col("rn")==1)

display(latest_condition)

# COMMAND ----------

from pyspark.sql.functions import col

conditions.filter(col("PATIENT") == "00126cb9-8460-4747-e302-c3609684531e") \
          .orderBy(col("START").desc()) \
          .display()

# COMMAND ----------

encounters.printSchema()

# COMMAND ----------

conditions.printSchema()

# COMMAND ----------

from pyspark.sql.functions import sum

total_cost = encounters.groupBy("PATIENT").agg(
    sum("TOTAL_CLAIM_COST").alias("TotalCost")
)

display(total_cost)

# COMMAND ----------

from pyspark.sql.functions import col

latest_condition_selected = latest_condition.select(
    col("PATIENT").alias("condition_patient"),
    col("DESCRIPTION").alias("Condition")
)

total_cost_selected = total_cost.select(
    col("PATIENT").alias("cost_patient"),
    col("TotalCost")
)

master_view = patients \
    .join(latest_condition_selected,
          patients.Id == latest_condition_selected.condition_patient,
          how="left") \
    .join(total_cost_selected,
          patients.Id == total_cost_selected.cost_patient,
          how="left") \
    .drop("condition_patient", "cost_patient")

# COMMAND ----------

display(master_view)

# COMMAND ----------

master_view = master_view.select(
    "Id",
    "FIRST",
    "LAST",
    "GENDER",
    "BIRTHDATE",
    "CITY",
    "STATE",
    "Condition",
    "TotalCost"
)
display(master_view)

# COMMAND ----------

# MAGIC %md
# MAGIC # **2. Security PII**
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import sha2, col, lit

# COMMAND ----------

patients_masked = patients.withColumn(
    "Hashed_ID",
    sha2(col("Id"), 256)
)

patients_masked = patients_masked \
    .withColumn("FIRST", lit("****")) \
    .withColumn("LAST", lit("****")) \
    .withColumn("SSN", lit("********")) \
    .withColumn("ADDRESS", lit("********"))

display(patients_masked)

# COMMAND ----------

conditions_masked = conditions.withColumn(
    "Hashed_ID",
    sha2(col("PATIENT"), 256)
)

display(conditions_masked)

# COMMAND ----------

encounters_masked = encounters.withColumn(
    "Hashed_ID",
    sha2(col("PATIENT"), 256)
)

display(encounters_masked)

# COMMAND ----------

patients_secure = patients_masked.select(
    "Hashed_ID",
    "BIRTHDATE",
    "GENDER",
    "CITY",
    "STATE",
    "HEALTHCARE_EXPENSES",
    "HEALTHCARE_COVERAGE"
)

display(patients_secure)

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Evolution**

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# Check if the new column exists
if "Vaccination_Status" not in patients.columns:
    patients = patients.withColumn(
        "Vaccination_Status",
        lit("Unknown")
    )

display(patients)

# COMMAND ----------

patients = patients.dropDuplicates(["Id"])

display(patients)

# COMMAND ----------

patients.printSchema()

# COMMAND ----------

