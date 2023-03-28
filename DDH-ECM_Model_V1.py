# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ECM Model - Version 1
# MAGIC 
# MAGIC Python Implementation

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

from scipy.optimize import curve_fit
from scipy.optimize import OptimizeWarning
from sklearn.metrics import mean_squared_error

from milkbot.formulas import ecm_milk, milkbot_, milkbot, error_allowance, estimate_milkbot_params

import pyspark.sql.functions as fn
from pyspark.sql.types import DoubleType, ArrayType, StringType
from pyspark.sql.window import Window
from pyspark.ml.feature import QuantileDiscretizer

# COMMAND ----------

# Model Parameters
id_vars = ['BDAT','ID','LAGR']
selected_herd = '1183'

# Interpolation Windows
windowIDpre  = Window.partitionBy("BDAT","ID","LACT").orderBy("DIM").rowsBetween(Window.unboundedPreceding, Window.currentRow)
windowIDpost = Window.partitionBy("BDAT","ID","LACT").orderBy("DIM").rowsBetween(Window.currentRow, Window.unboundedFollowing)

# Translation of Python Functions to PySpark UDF
ecm_udf = fn.udf(ecm_milk, DoubleType())
sigma_udf = fn.udf(error_allowance(), DoubleType())

# COMMAND ----------

spark.read.format('delta').load('abfss://governed@devazrddp01adls.dfs.core.windows.net/agb/FoundationTables/TestDayEvents/').printSchema()

# COMMAND ----------

df = spark.read.format('delta').load('abfss://governed@devazrddp01adls.dfs.core.windows.net/agb/FoundationTables/TestDayEvents/') \
  .where(f"SourceHerdID = {selected_herd}") \
  .withColumnRenamed('MilkWeight','MILK')\
  .withColumnRenamed('PercentFat','PCTF')\
  .withColumnRenamed('PercentProtein','PCTP')\
  .withColumnRenamed('BirthDate','BDAT')\
  .withColumnRenamed('LactationNumber','LACT')\
  .withColumn('LAGR', fn.when(fn.col('LACT') < 4, fn.col('LACT').cast(StringType())).otherwise(fn.lit("4+")))\
  .withColumnRenamed('EarTag','ID')\
  .where("MILK is not null and dim is not null")\
  .withColumn('PCTF',fn.last(fn.col('PCTF'),ignorenulls=True).over(windowIDpre))\
  .withColumn('PCTF',fn.first(fn.col('PCTF'),ignorenulls=True).over(windowIDpost))\
  .withColumn('PCTP',fn.last(fn.col('PCTP'),ignorenulls=True).over(windowIDpre))\
  .withColumn('PCTP',fn.first(fn.col('PCTP'),ignorenulls=True).over(windowIDpost))\
  .where("PCTF is not null and PCTP is not null")\
  .withColumn("ECM", ecm_udf(fn.col("MILK"),fn.col("PCTF"),fn.col("PCTP")))\
  .withColumn("SIGMA", sigma_udf(fn.col("DIM")))\
  .select('BDAT','ID','LAGR','LACT','MILK','DIM','PCTF','PCTP','ECM','SIGMA')\
  .sort(*id_vars, 'DIM')

print(df.count())

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# Animals with Long Enough Lactation History
suff_hist = df.groupby(*id_vars).agg(fn.min('DIM').alias('DIM_MIN'), fn.max('DIM').alias('DIM_MAX'))\
  .where("DIM_MIN >= 0 and DIM_MAX >= 200 and DIM_MAX < 350")

suff_hist.groupby('LAGR').count().sort('LAGR').show()

# COMMAND ----------

med = df.join(suff_hist, on=id_vars, how='inner')\
  .groupby('LAGR','DIM').agg(fn.percentile_approx("ECM", 0.50).alias("ECM_MEDIAN"))\
  .withColumn("SIGMA", fn.when(fn.col("DIM") < 15, fn.lit(1.5)).otherwise(fn.lit(1.0)))\
  .cache()

display(med)

# COMMAND ----------

# Results in Python

dim = np.arange(350, dtype=np.float64)
p0 = np.array([90.0, 10.0, -5.0, 0.001])
bounds = (
    [0., 0., -500., 0.], 
    [500., 500., 600., 1.0]
    )
res = med.groupby("LAGR").agg(fn.collect_list('DIM').alias('dim'),fn.collect_list('ECM_MEDIAN').alias('ecm')).sort("LAGR").collect()
fig, ax = plt.subplots(4,1,figsize=(8,15), sharey=True, sharex=True)
colors = {'1':'b','2':'g','3':'r','4+':'k'}
for i, r in enumerate(res):
  popt, _ = curve_fit(milkbot_, r.dim, r.ecm, p0=p0, bounds=bounds)
  print(r.LAGR, popt)
  y_est = milkbot_(dim,*popt)
  ax[i].plot(dim,y_est,colors[r.LAGR]+"-", label="Estimated")
  ax[i].plot(r.dim,r.ecm,colors[r.LAGR]+'o', alpha=0.3, label="Measured")
  ax[i].grid()
  ax[i].legend()
  ax[i].set_title(r.LAGR)
plt.show()

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

mbot = med.groupby("LAGR").agg(
    fn.collect_list('DIM').alias('dim'), 
    fn.collect_list('ECM_MEDIAN').alias('ecm'),
    fn.collect_list('SIGMA').alias('sigma'),
).withColumn("milkbot_pars", milkbot_ez_udf(fn.col('dim'),fn.col('ecm'),fn.col("sigma")))

for r in mbot.select("LAGR","milkbot_pars").sort("LAGR").collect():
  print("Lactation Group {:2} -> a:{:7.1f}, b:{:7.1f}, c:{:7.1f}, d:{:.4f}".format(r.LAGR, *r.milkbot_pars))

# COMMAND ----------

med.join(mbot.select("LAGR","milkbot_pars") , on="LAGR", how="inner")\
  .withColumn("ecm_est", milkbot_est(fn.col('dim'),fn.col('milkbot_pars')))\
  .display()

# COMMAND ----------

qds1 = QuantileDiscretizer(inputCol="ecm_avg", outputCol="ecm_qtile")
qds1.setNumBuckets(4)
qds1.setRelativeError(0.01)
qds1.setHandleInvalid("error")


avg_ecm = df.join(suff_hist, on=id_vars, how='inner')\
  .groupby(id_vars).agg(fn.avg("ECM").alias("ecm_avg"))

bucketizer = qds1.fit(avg_ecm)


display(bucketizer.transform(avg_ecm))

# COMMAND ----------

df_by_tile = df.join(suff_hist, on=id_vars, how='inner')\
  .join(bucketizer.transform(avg_ecm).select(*id_vars, 'ecm_qtile'), on=id_vars, how='inner').cache()
  #.groupby('LAGR','ecm_qtile','DIM').agg(fn.percentile_approx("ECM", 0.50).alias("ECM_MEDIAN"))\
  #.withColumn("SIGMA", fn.when(fn.col("DIM") < 15, fn.lit(1.5)).otherwise(fn.lit(1.0)))\
  #.cache()

# COMMAND ----------

display(df_by_tile.where("LAGR = '1'"))

# COMMAND ----------

mbot2 = df.join(suff_hist, on=id_vars, how='inner').groupby("LAGR").agg(
    fn.collect_list('DIM').alias('dim'), 
    fn.collect_list('ECM').alias('ecm'),
    fn.collect_list('SIGMA').alias('sigma'),
).withColumn("milkbot_pars", milkbot_ez_udf(fn.col('dim'),fn.col('ecm'), fn.col("sigma")))

# COMMAND ----------

print("MBOT 1")
for r in mbot.select("LAGR","milkbot_pars").sort("LAGR").collect():
  print("Lactation Group {:2} -> a:{:7.1f}, b:{:7.1f}, c:{:7.1f}, d:{:.4f}".format(r.LAGR, *r.milkbot_pars))
  
print("MBOT 2")
for r in mbot2.select("LAGR","milkbot_pars").sort("LAGR").collect():
  print("Lactation Group {:2} -> a:{:7.1f}, b:{:7.1f}, c:{:7.1f}, d:{:.4f}".format(r.LAGR, *r.milkbot_pars))

# COMMAND ----------

out = df.join(suff_hist, on=id_vars, how='inner')\
  .join(mbot2.select("LAGR","milkbot_pars") , on="LAGR", how="inner")\
  .withColumn("ecm_est", milkbot_est(fn.col('dim'),fn.col('milkbot_pars')))

display(out)

# COMMAND ----------


