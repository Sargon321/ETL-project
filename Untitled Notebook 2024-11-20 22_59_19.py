# Databricks notebook source
# Ange filens sökväg
file_path = "dbfs:/user/hive/warehouse/season_1617"

# Läs in filen till en Spark DataFrame med Delta-format
df = spark.read.format("delta").load(file_path)

# Visa de första raderna
display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print(f"Rader: {df.count()}, Kolumner: {len(df.columns)}")


# COMMAND ----------

df.show(15)


# COMMAND ----------

df.describe().show()


# COMMAND ----------

from pyspark.sql.functions import col

# Kontrollera null-värden per kolumn
df.select([col(c).isNull().alias(c) for c in df.columns]).show()


# COMMAND ----------

df.select("HomeTeam").distinct().show()