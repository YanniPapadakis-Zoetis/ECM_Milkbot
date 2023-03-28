# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ECM Milkbot - V1
# MAGIC 
# MAGIC PySpark Implementation
# MAGIC 
# MAGIC Model Originally Developed in R

# COMMAND ----------

import numpy as np

from scipy.optimize import curve_fit
from scipy.optimize import OptimizeWarning
from sklearn.metrics import mean_squared_error

import pyspark.sql.functions as fn
from pyspark.sql.types import DoubleType, ArrayType, StringType
from pyspark.sql.window import Window
from pyspark.ml.feature import QuantileDiscretizer

from milkbot.formulas import ecm_milk, milkbot_, milkbot, error_allowance, estimate_milkbot_params

# Case Identification Keys
id_vars = ["BDAT","ID","LACT"]

# COMMAND ----------

# Translation of Python Functions to PySpark UDF
ecm_udf = fn.udf(ecm_milk, DoubleType())
sigma_udf = fn.udf(error_allowance(), DoubleType())

# COMMAND ----------

# MAGIC %md ## Get Model Inputs

# COMMAND ----------

# MAGIC %run ./DDH-ECM_Model_V1_INPUTS $LastECMRunDate="2023-03-01"

# COMMAND ----------

df = spark.sql("select * from ECMINPUTS where SourceHerdID = '10'")

df.printSchema()

# COMMAND ----------

df.describe().display()

# COMMAND ----------

display(df.sort("ID","BDAT","LAGR","LACT","DIM"))

# COMMAND ----------

# MAGIC %md # Herd & Lactaction Group Estimates

# COMMAND ----------

### count number of test days for each cow, 
### only use cows with at least 2 test days beyond 200DIM for base curve 
###   to ensure the base curve represents the ENTIRE lactation

accept = df.where("ECM is not null and ECM > 0")\
  .where("DIM >= 200 and DIM < 350")\
  .groupby(*id_vars)\
  .count()\
  .where("count >= 2")

accept.count()

# COMMAND ----------

df0 = df.join(accept.drop("count"), on=id_vars, how='inner')\
  .where("ECM is not null and DIM < 325")\
  .withColumn("SIGMA", sigma_udf(fn.col("DIM")))\
  .cache()

# COMMAND ----------

df0.describe().display()

# COMMAND ----------

p0 = np.array([100.0, 10.0, -1.0, 0.001])
bounds = (
    [0., 10., -5., 0.0001], 
    [500., 50., 5., .002]
    )

def estimate_milkbot_params_ez(t, y, s):
    popt, _ = curve_fit(milkbot_, t, y, sigma=s, p0=p0, bounds=bounds)
    return popt.tolist()

milkbot_ez_udf = fn.udf(estimate_milkbot_params_ez, ArrayType(DoubleType()))

milkbot_est = fn.udf(lambda dim, par: float(milkbot_(dim, *par)), DoubleType())

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
groups = ["HERD","LAGR_1","LAGR_2","LAGR_3","LAGR_4",]
for r in mbot.select("LAGR","milkbot_pars").sort("LAGR").collect():
  lagrs.append(r.LAGR)
  print("{:10} -> a:{:7.1f}, b:{:7.1f}, c:{:7.1f}, d:{:10.4f}".format(groups[r.LAGR], *r.milkbot_pars))

# COMMAND ----------

group_name = fn.udf(lambda i: groups[i], StringType())
X = spark.createDataFrame(
  data=[(g, dim) for g in lagrs for dim in range(0,350,5)], 
  schema = ["LAGR","dim"])\
  .join(mbot.select("LAGR","milkbot_pars") , on="LAGR", how="inner")\
  .withColumn("group", group_name(fn.col("LAGR")))\
  .withColumn("ecm_est", milkbot_est(fn.col('dim'),fn.col('milkbot_pars')))

# COMMAND ----------

display(X)

# COMMAND ----------


