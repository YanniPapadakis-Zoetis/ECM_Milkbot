# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ECM Milkbot - V1
# MAGIC 
# MAGIC PySpark Implementation
# MAGIC 
# MAGIC Model Originally Developed in R

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

import numpy as np

from scipy.optimize import curve_fit
from scipy.optimize import OptimizeWarning
from sklearn.metrics import mean_squared_error

import pyspark.sql.functions as fn
from pyspark.sql.types import DoubleType, ArrayType, StringType

from milkbot.formulas import milkbot_
from milkbot.udfs import ecm_udf, sigma_udf, milkbot_ez_udf, milkbot_est
from milkbot import param


print(param["id_vars"])

# COMMAND ----------

# MAGIC %md ## Get Model Inputs

# COMMAND ----------

# MAGIC %run ./DDH-ECM_Model_V1_INPUTS $LastECMRunDate="2023-03-01", $HerdID="10"

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

accept = df.where("ECM is not null and ECM > 0")\
  .where("DIM >= 200 and DIM < 350")\
  .groupby(*param["id_vars"])\
  .count()\
  .where("count >= 2")

accept.count()

# COMMAND ----------

df0 = df.join(accept.drop("count"), on=param["id_vars"], how='inner')\
  .where("ECM is not null and DIM < 325")\
  .withColumn("SIGMA", sigma_udf(fn.col("DIM")))\
  .cache()

# COMMAND ----------

df0.describe().display()

# COMMAND ----------
mbot_lagr = df0.groupby("LAGR").agg(
      fn.collect_list('DIM').alias('dim'), 
      fn.collect_list('ECM').alias('ecm'),
      fn.collect_list('SIGMA').alias('sigma'),
    ).withColumn("milkbot_pars", milkbot_ez_udf(fn.col('dim'),fn.col('ecm'),fn.col("sigma")))\
  .select("LAGR","milkbot_pars")

mbot_herd = df0.groupby().agg(
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

group_name = fn.udf(lambda i: param["hlgroups"][i], StringType())
X = spark.createDataFrame(
  data=[(g, dim) for g in lagrs for dim in range(0,350,5)], 
  schema = ["LAGR","dim"])\
  .join(mbot.select("LAGR","milkbot_pars") , on="LAGR", how="inner")\
  .withColumn("group", group_name(fn.col("LAGR")))\
  .withColumn("ecm_est", milkbot_est(fn.col('dim'),fn.col('milkbot_pars')))

# COMMAND ----------

display(X)

# COMMAND ----------


