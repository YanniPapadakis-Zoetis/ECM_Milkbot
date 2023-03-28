# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ECM Model - Version 0
# MAGIC 
# MAGIC Python Implementation

# COMMAND ----------

dbutils.fs.ls('abfss://governed@devazrddp01adls.dfs.core.windows.net/agb/FoundationTables/')

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

from scipy.optimize import curve_fit
from scipy.optimize import OptimizeWarning
from sklearn.metrics import mean_squared_error

from milkbot.formulas import ecm_milk, milkbot_, error_allowance, estimate_milkbot_params

# COMMAND ----------

raw=spark.read.format('delta') \
.load('abfss://governed@devazrddp01adls.dfs.core.windows.net/agb/FoundationTables/TestDayEvents/') \
.where("SourceHerdID = 1183") \
.sort("EarTag","LactationNumber","DIM")
raw.display()

# COMMAND ----------

raw.printSchema()

# COMMAND ----------

import pyspark.sql.functions as fn
from pyspark.sql.types import DoubleType, ArrayType
from pyspark.sql.window import Window
windowIDpre  = Window.partitionBy("BDAT","EarTag","LACT").orderBy("DIM").rowsBetween(Window.unboundedPreceding, Window.currentRow)
windowIDpost = Window.partitionBy("BDAT","EarTag","LACT").orderBy("DIM").rowsBetween(Window.currentRow, Window.unboundedFollowing)

ecm_udf = fn.udf(ecm_milk, DoubleType())
sigma_udf = fn.udf(error_allowance(), DoubleType())

# COMMAND ----------

id_vars = ['BDAT','EarTag','LACT']
df = raw.withColumnRenamed('MilkWeight','MILK')\
  .withColumnRenamed('PercentFat','PCTF')\
  .withColumnRenamed('PercentProtein','PCTP')\
  .withColumnRenamed('BirthDate','BDAT')\
  .withColumnRenamed('LactationNumber','LACT')\
  .where("MILK is not null and dim is not null and dim > 0 and dim < 400")\
  .withColumn('PCTF',fn.last(fn.col('PCTF'),ignorenulls=True).over(windowIDpre))\
  .withColumn('PCTF',fn.first(fn.col('PCTF'),ignorenulls=True).over(windowIDpost))\
  .withColumn('PCTP',fn.last(fn.col('PCTP'),ignorenulls=True).over(windowIDpre))\
  .withColumn('PCTP',fn.first(fn.col('PCTP'),ignorenulls=True).over(windowIDpost))\
  .select('BDAT','EarTag','LACT','MILK','DIM','PCTF','PCTP')

npoints = df.groupby(id_vars).count().withColumnRenamed("count","npoints")

df = df.join(npoints.where("npoints >= 4"), on=id_vars, how='inner')\
  .where("PCTF is not null and PCTP is not null")\
  .withColumn("ECM", ecm_udf(fn.col("MILK"),fn.col("PCTF"),fn.col("PCTP")))\
  .withColumn("SIGMA", sigma_udf(fn.col("DIM")))\
  .sort('BDAT','EarTag','LACT','DIM')

display(df)

# COMMAND ----------

display(df.describe())

# COMMAND ----------

p0 = np.array([90.0, 50.0, -5.0, 0.001])
bounds = (
    [0., 10., -5., 0.], 
    [500., 50., 6., 0.002]
    )   

def milkbot_est(t,y,s):
  return estimate_milkbot_params(np.array(t),np.array(y),np.array(s),p0=p0,bounds=bounds).tolist()

milkbot_udf = fn.udf(milkbot_est, ArrayType(DoubleType()))

def milkbot_error(t,y,p):
  y_est = milkbot_(np.array(t),*p)
  return (np.array(y)-y_est).tolist()

milkbot_error_udf = fn.udf(milkbot_error, ArrayType(DoubleType()))

array_rmse = fn.udf(lambda x: float(np.sqrt(np.square(x).mean())), DoubleType())

# COMMAND ----------

mbot = df.where("npoints > 4")\
  .groupby(id_vars).agg(
    fn.collect_list('DIM').alias('dim'), 
    fn.collect_list('ECM').alias('ecm'), 
    fn.collect_list('SIGMA').alias('sigma')
).withColumn("milkbot_pars", milkbot_udf(fn.col('dim'),fn.col('ecm'),fn.col('sigma')))\
 .withColumn("error", milkbot_error_udf(fn.col('dim'),fn.col('ecm'),fn.col('milkbot_pars')))\
 .withColumn("rmse", array_rmse("error"))

mbot.cache()

display(mbot)

# COMMAND ----------

abcd = mbot.rdd.map(lambda x: (x.BDAT, x.EarTag, x.LACT, x.milkbot_pars[0],x.milkbot_pars[1],x.milkbot_pars[2],x.milkbot_pars[3],x.rmse))\
 .toDF(['BDAT', 'EarTag', 'LACT', "a", "b", "c", "d", "rmse"])

display(abcd.where("rmse > 15"))

# COMMAND ----------

abcd.withColumn("no_convergence", fn.when(fn.col("a") < 1.0, 1).otherwise(0)).describe().display()

# COMMAND ----------

mbot.where("milkbot_pars[0] > 1.0").withColumn("diff", fn.explode("error")).select('EarTag', "diff").describe().display()

# COMMAND ----------

dim = np.arange(400)
for r in mbot.where("rmse < 5.0").select("milkbot_pars").rdd.takeSample(True,100,23):
  p = r.milkbot_pars
  y_est = milkbot_(dim,*p)
  if y_est.min() > 0:
    plt.plot(dim,y_est,'k-',alpha=0.3)
  else:
    print(p)
    print(y_est[:30])
  
plt.grid()
plt.show()

# COMMAND ----------

for r in mbot.where("rmse > 20.0").rdd.takeSample(True,100,23):
  p = r.milkbot_pars
  y_est = milkbot_(dim,*p)
  if y_est.min() > 0:
    plt.plot(dim,y_est,'k-',alpha=0.3)
  else:
    print(p)
    print(y_est[:30])
  
plt.grid()
plt.show()

# COMMAND ----------

for r in mbot.where("milkbot_pars[1] > 40.").select("milkbot_pars").rdd.takeSample(True,100,23):
  p = r.milkbot_pars
  y_est = milkbot_(dim,*p)
  if y_est.min() > 0:
    plt.plot(dim,y_est,'k-',alpha=0.3)
  else:
    print(p)
    print(y_est[:30])
  
plt.grid()
plt.show()

# COMMAND ----------

for r in mbot.where(fn.element_at(fn.col('dim'), -1 ) < 150.0).select("milkbot_pars").take(100):
  p = r.milkbot_pars
  y_est = milkbot_(dim,*p)
  if y_est.min() > 0:
    plt.plot(dim,y_est,'k-',alpha=0.3)
  else:
    print(p)
    print(y_est[:30])
  
plt.grid()
plt.show()

# COMMAND ----------

for r in mbot.where(fn.element_at(fn.col('dim'), -1 ) > 300.0).select("milkbot_pars").rdd.takeSample(True,100,23):
  p = r.milkbot_pars
  y_est = milkbot_(dim,*p)
  if y_est.min() > 0:
    plt.plot(dim,y_est,'k-',alpha=0.3)
  else:
    print(p)
    print(y_est[:30])
  
plt.grid()
plt.show()

# COMMAND ----------

for r in mbot.where(fn.size(fn.col('dim')) < 6).select("milkbot_pars").rdd.takeSample(True,100,23):
  p = r.milkbot_pars
  y_est = milkbot_(dim,*p)
  if y_est.min() > 0:
    plt.plot(dim,y_est,'k-',alpha=0.3)
  else:
    print(p)
    print(y_est[:30])
  
plt.grid()
plt.show()

# COMMAND ----------

for r in mbot.where(fn.size(fn.col('dim')) > 10).select("milkbot_pars").rdd.takeSample(True,100,23):
  p = r.milkbot_pars
  y_est = milkbot_(dim,*p)
  if y_est.min() > 0:
    plt.plot(dim,y_est,'k-',alpha=0.3)
  else:
    print(p)
    print(y_est[:30])
  
plt.grid()
plt.show()

# COMMAND ----------


