# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ECM Milkbot - V1
# MAGIC 
# MAGIC PySpark Implementation
# MAGIC 
# MAGIC Model Originally Developed in R

# COMMAND ----------

import pyspark.sql.functions as fn
from milkbot.udfs import ecm_udf, sigma_udf, milkbot_ez_udf, milkbot_est, group_name_udf
from milkbot import param

# COMMAND ----------

# MAGIC %md ## Get Model Inputs

# COMMAND ----------

# MAGIC %run ./DDH-ECM_Model_V1_INPUTS $LastECMRunDate="2023-03-01" $HerdID="10"

# COMMAND ----------

df = spark.sql(param["input_sql"])

df.printSchema()

# COMMAND ----------

df.describe().display()

# COMMAND ----------

display(df.sort(param["sort_keys"]))

# COMMAND ----------

# MAGIC %md # Herd & Lactaction Group Estimates

# COMMAND ----------

### count number of test days for each cow, 
### only use cows with at least 2 test days beyond 200DIM for base curve 
###   to ensure the base curve represents the ENTIRE lactation

accept = df.where(param["accept"]["ecm_filter"])\
  .where(param["accept"]["dim_filter"])\
  .groupby(*param["id_vars"])\
  .count()\
  .where(param["accept"]["count_filter"])

accept.count()

# COMMAND ----------

hlgdf = df.join(accept.drop("count"), on=param["id_vars"], how='inner')\
  .where(param["hlgdf"]["filter"])\
  .withColumn("SIGMA", sigma_udf(fn.col("DIM")))\
  .cache()

# COMMAND ----------

hlgdf.describe().display()

# COMMAND ----------

mbot_lagr = hlgdf.groupby("LAGR").agg(
      fn.collect_list('DIM').alias('dim'), 
      fn.collect_list('ECM').alias('ecm'),
      fn.collect_list('SIGMA').alias('sigma'),
    ).withColumn("milkbot_pars", milkbot_ez_udf(fn.col('dim'),fn.col('ecm'),fn.col("sigma")))\
  .select("LAGR","milkbot_pars")

mbot_herd = hlgdf.groupby().agg(
      fn.collect_list('DIM').alias('dim'), 
      fn.collect_list('ECM').alias('ecm'),
      fn.collect_list('SIGMA').alias('sigma'),
    ).withColumn("milkbot_pars", milkbot_ez_udf(fn.col('dim'),fn.col('ecm'),fn.col("sigma")))\
  .withColumn("LAGR", fn.lit(0))\
  .select("LAGR","milkbot_pars")

mbot = mbot_herd.union(mbot_lagr)

lagrs = []
for r in mbot.select("LAGR","milkbot_pars").sort("LAGR").collect():
  lagrs.append(r.LAGR)
  print(param["abcd_fmt"].format(param["hlgroups"][r.LAGR], *r.milkbot_pars))

# COMMAND ----------

X = spark.createDataFrame(
  data=[(g, dim) for g in lagrs for dim in range(0,350,5)], 
  schema = ["LAGR","dim"])\
  .join(mbot.select("LAGR","milkbot_pars") , on="LAGR", how="inner")\
  .withColumn("group", group_name_udf(fn.col("LAGR")))\
  .withColumn("ecm_est", milkbot_est(fn.col('dim'),fn.col('milkbot_pars')))

# COMMAND ----------

display(X)

# COMMAND ----------


